"""The batch kernel on any OpenCL GPU, through PyOpenCL: `_gpu`'s source compiled as OpenCL C.

Everything the kernel decides, and why it is laid out as it is, is in :mod:`_gpu`; this is the
same source `_mlx` runs on Metal and `_cupy` on CUDA. What is here is OpenCL's spelling of the few
things the compilers name differently, the choice of device, and the launch.

It is the backend for GPUs that have neither -- Intel Arc, AMD -- and it runs on NVIDIA and Apple
too, through their OpenCL drivers. float32, as on the other two: many of these GPUs have no double
precision, and the point is the same ranking.

The device is the first GPU any OpenCL platform offers, or the one ``PYOPENCL_CTX`` names
(``"0:1"`` -- platform 0, device 1), as PyOpenCL reads it everywhere else.
"""
from __future__ import annotations

import os

import numpy as np

from ziplime.vectorized.kernel import _gpu
from ziplime.vectorized.kernel.costs import ResolvedCommission, ResolvedSlippage
from ziplime.vectorized.kernel.models import VectorKernelError

try:  # pragma: no cover - depends on the platform
    import pyopencl as cl

    HAVE_PYOPENCL = True
except ImportError:  # pragma: no cover
    cl = None
    HAVE_PYOPENCL = False

_NAME = "ziplime_vector_batch"

_SOURCE = """
typedef uint u32;
typedef ulong u64;
#define ZL_ABS fabs
#define ZL_MAX fmax
#define ZL_MIN fmin
""" + _gpu.CONSTANTS + f"""
__kernel void {_NAME}(
        __global const uchar* restrict signals, __global const uchar* restrict flags,
        __global const float* restrict execution, __global const float* restrict marks,
        __global const float* restrict sizes, __global const float* restrict volumes,
        __global const int* restrict ints, __global const float* restrict floats,
        __global float* restrict values, __global float* restrict held,
        __global int* restrict counts, __global float* restrict money) {{
    const u32 combo = get_global_id(0);
""" + _gpu.BODY + """
}
"""

#: (context, queue, kernel), built on the first run.
_state = None


def _gpu_devices() -> list:
    devices = []
    try:
        for platform in cl.get_platforms():
            try:
                devices += platform.get_devices(device_type=cl.device_type.GPU)
            except cl.Error:  # a platform with no GPU raises rather than returning none
                continue
    except cl.Error:  # no OpenCL driver installed at all
        return []
    return devices


def _context():
    """The context ``PYOPENCL_CTX`` asks for, else one on the first GPU."""
    if os.environ.get("PYOPENCL_CTX"):
        try:
            return cl.create_some_context(interactive=False)
        except Exception as error:  # PyOpenCL raises its own types, and a bare RuntimeError
            raise VectorKernelError(
                f"backend='opencl' could not open the device PYOPENCL_CTX="
                f"{os.environ['PYOPENCL_CTX']!r} names: {error}. Check `clinfo` for the "
                f"platform:device numbers, or unset it to use the first GPU.") from None
    return cl.Context([_gpu_devices()[0]])


def require() -> None:
    """Refuse, by name, an OpenCL run on a machine that cannot do one."""
    if not HAVE_PYOPENCL:
        raise VectorKernelError(
            "backend='opencl' needs PyOpenCL, which is not installed here: `pip install "
            "\"ziplime[opencl]\"`, with the GPU vendor's OpenCL driver (for Intel Arc on Linux, "
            "intel-opencl-icd). On an Apple silicon Mac backend='mlx' is faster; on NVIDIA "
            "backend='cupy'; without a GPU backend='exact', which gives the same decisions in "
            "float64, one combination at a time.")
    if not os.environ.get("PYOPENCL_CTX") and not _gpu_devices():
        raise VectorKernelError(
            "backend='opencl' found PyOpenCL but no OpenCL GPU: install the GPU vendor's OpenCL "
            "driver (for Intel Arc on Linux, intel-opencl-icd) and check that `clinfo` lists the "
            "GPU, or name a device with PYOPENCL_CTX. Without a GPU use backend='exact'.")


def _compiled():
    global _state
    if _state is None:
        context = _context()
        program = cl.Program(context, _SOURCE).build()
        _state = context, cl.CommandQueue(context), cl.Kernel(program, _NAME)
    return _state


def _packed_signals(stacks: dict[str, np.ndarray]) -> np.ndarray:
    """Four boolean ``(combinations, T, N)`` stacks as one byte per cell, combination innermost.

    Packed on the host: PyOpenCL has no array library to do it on the device, and the transfer is
    the packed byte either way.
    """
    n_combos, n_bars, n_columns = stacks["entries"].shape
    packed = np.zeros((n_bars, n_columns, n_combos), dtype=np.uint8)
    for name, intent in _gpu.SIGNAL_BITS:
        if stacks[name].any():
            packed |= stacks[name].transpose(1, 2, 0).astype(np.uint8) * np.uint8(1 << intent)
    return packed


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
    host["signals"] = _packed_signals(stacks)
    context, queue, kernel = _compiled()

    outputs = [np.empty(shape, dtype=dtype) for shape, dtype in
               zip(_gpu.output_shapes(n_combos, n_bars, n_columns),
                   (np.float32, np.float32, np.int32, np.float32))]
    largest = max(array.nbytes for array in [*host.values(), *outputs])
    limit = context.devices[0].max_mem_alloc_size
    if largest > limit:
        raise VectorKernelError(
            f"The batch needs a {largest / 2 ** 20:,.0f} MB buffer and this OpenCL device "
            f"allocates at most {limit / 2 ** 20:,.0f} MB in one. Split the combinations into "
            f"smaller batches.")

    read = cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR
    inputs = [cl.Buffer(context, read, hostbuf=np.ascontiguousarray(host[name]))
              for name in _gpu.INPUT_NAMES]
    written = [cl.Buffer(context, cl.mem_flags.WRITE_ONLY, output.nbytes) for output in outputs]

    # OpenCL 1.2 wants the global size a multiple of the group's; the extra threads return at once.
    blocks = -(-n_combos // _gpu.THREADGROUP)
    kernel(queue, (blocks * _gpu.THREADGROUP,), (_gpu.THREADGROUP,), *inputs, *written)
    for output, buffer in zip(outputs, written):
        cl.enqueue_copy(queue, output, buffer)
    queue.finish()
    return _gpu.summary(*outputs)
