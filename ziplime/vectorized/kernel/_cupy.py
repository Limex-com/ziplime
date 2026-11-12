"""The batch kernel on an NVIDIA or AMD GPU, through CuPy: `_gpu`'s source compiled for CUDA.

Everything the kernel decides, and why it is laid out as it is, is in :mod:`_gpu`; this is the
same source `_mlx` runs on Metal. What is here is CUDA's spelling of the few things the two
compilers name differently, and the launch.

CuPy built for ROCm runs the same source on an AMD GPU: its RawKernel compiles through HIP, which
reads CUDA's spelling as it is. The one thing that changes there is the block, which is a whole
wavefront -- 64 threads on AMD's compute GPUs -- rather than half of one.

CUDA has double precision and this still runs in float32: the point of the backend is the same
ranking `mlx` gives, and :meth:`~ziplime.vectorized.kernel.batch.BatchSimulationResult.rerun` is
where float64 comes from on either.
"""
from __future__ import annotations

import numpy as np

from ziplime.vectorized.kernel import _gpu
from ziplime.vectorized.kernel.costs import ResolvedCommission, ResolvedSlippage
from ziplime.vectorized.kernel.models import VectorKernelError

try:  # pragma: no cover - depends on the platform
    import cupy as cp

    HAVE_CUPY = True
except ImportError:  # pragma: no cover
    cp = None
    HAVE_CUPY = False

_NAME = "ziplime_vector_batch"

_SOURCE = """
typedef unsigned int u32;
typedef unsigned long long u64;
#define ZL_ABS fabsf
#define ZL_MAX fmaxf
#define ZL_MIN fminf
""" + _gpu.CONSTANTS + f"""
extern "C" __global__ void {_NAME}(
        const unsigned char* __restrict__ signals, const unsigned char* __restrict__ flags,
        const float* __restrict__ execution, const float* __restrict__ marks,
        const float* __restrict__ sizes, const float* __restrict__ volumes,
        const int* __restrict__ ints, const float* __restrict__ floats,
        float* __restrict__ values, float* __restrict__ held, int* __restrict__ counts,
        float* __restrict__ money) {{
    const u32 combo = blockIdx.x * blockDim.x + threadIdx.x;
""" + _gpu.BODY + """
}
"""

#: Threads per block under ROCm: one wavefront on CDNA (MI-series), two on RDNA (Radeon).
HIP_BLOCK = 64

_kernel = None


def is_rocm() -> bool:
    """CuPy built for ROCm (HIP) rather than CUDA."""
    return bool(getattr(cp.cuda.runtime, "is_hip", False))


def _device_count() -> int:
    try:
        return cp.cuda.runtime.getDeviceCount()
    except cp.cuda.runtime.CUDARuntimeError:
        return 0


def require() -> None:
    """Refuse, by name, a CuPy run on a machine that cannot do one."""
    if not HAVE_CUPY:
        raise VectorKernelError(
            "backend='cupy' needs CuPy, which is not installed here: `pip install "
            "\"ziplime[cuda]\"` on a machine with an NVIDIA GPU, or CuPy built for ROCm on Linux "
            "with an AMD GPU. On an Apple silicon Mac use backend='mlx'; on any other GPU "
            "backend='opencl'; without one backend='exact', which gives the same decisions in "
            "float64, one combination at a time.")
    if not _device_count():
        if is_rocm():
            raise VectorKernelError(
                "backend='cupy' found CuPy for ROCm but no AMD GPU: check that `rocm-smi` lists "
                "the GPU and that CuPy was built for this ROCm. Without a GPU use "
                "backend='exact'.")
        raise VectorKernelError(
            "backend='cupy' found CuPy but no CUDA device: check that `nvidia-smi` lists the GPU "
            "and that the CuPy wheel matches the driver's CUDA (cupy-cuda12x for CUDA 12 and "
            "later drivers). Without a GPU use backend='exact'.")


def _compiled():
    global _kernel
    if _kernel is None:
        _kernel = cp.RawKernel(_SOURCE, _NAME)
    return _kernel


def _packed_signals(stacks: dict[str, np.ndarray]):
    """Four boolean ``(combinations, T, N)`` stacks as one byte per cell, combination innermost."""
    packed = cp.zeros(stacks["entries"].shape, dtype=cp.uint8)
    for name, intent in _gpu.SIGNAL_BITS:
        # A long-only sweep leaves both short stacks empty; copying them over is most of the
        # transfer for nothing.
        if stacks[name].any():
            packed |= cp.asarray(stacks[name]).astype(cp.uint8) * np.uint8(1 << intent)
    return cp.ascontiguousarray(cp.transpose(packed, (1, 2, 0)))


def run_batch(stacks: dict[str, np.ndarray], execution_values: np.ndarray,
              close_values: np.ndarray, size_values: np.ndarray,
              volume_values: np.ndarray | None, slippage: ResolvedSlippage,
              commission: ResolvedCommission, initial_cash: float, accumulate: bool,
              bar_offset: int) -> dict[str, np.ndarray]:
    """Every combination through the kernel at once; returns :func:`_gpu.summary`."""
    require()
    n_combos, n_bars, n_columns = stacks["entries"].shape
    host = _gpu.host_inputs(execution_values, close_values, size_values, volume_values, slippage,
                            commission, initial_cash, accumulate, bar_offset, n_combos)
    inputs = [_packed_signals(stacks)] + [cp.ascontiguousarray(cp.asarray(host[name]))
                                          for name in _gpu.INPUT_NAMES[1:]]
    outputs = [cp.empty(shape, dtype=dtype) for shape, dtype in
               zip(_gpu.output_shapes(n_combos, n_bars, n_columns),
                   (cp.float32, cp.float32, cp.int32, cp.float32))]

    block = HIP_BLOCK if is_rocm() else _gpu.THREADGROUP
    _compiled()((-(-n_combos // block),), (block,), (*inputs, *outputs))
    return _gpu.summary(*(output.get() for output in outputs))
