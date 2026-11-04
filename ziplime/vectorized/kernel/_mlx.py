"""The batch kernel on the Apple GPU, through MLX: `_gpu`'s source compiled for Metal.

Everything the kernel decides, and why it is laid out as it is, is in :mod:`_gpu`. What is here is
Metal's spelling of the few things the two compilers name differently, and the launch.

MLX also runs on NVIDIA (``mlx[cuda]`` on Linux), but a Metal kernel does not, so this backend
asks for a Metal device rather than for the package. On NVIDIA the same source runs through
CuPy: ``backend="cupy"``.
"""
from __future__ import annotations

import numpy as np

from ziplime.vectorized.kernel import _gpu
from ziplime.vectorized.kernel.costs import ResolvedCommission, ResolvedSlippage
from ziplime.vectorized.kernel.models import VectorKernelError

try:  # pragma: no cover - depends on the platform
    import mlx.core as mx

    HAVE_MLX = True
except ImportError:  # pragma: no cover
    mx = None
    HAVE_MLX = False

_HEADER = """
typedef uint u32;
typedef ulong u64;
#define ZL_ABS metal::fabs
#define ZL_MAX metal::max
#define ZL_MIN metal::min
""" + _gpu.CONSTANTS

_SOURCE = """
    const u32 combo = thread_position_in_grid.x;
""" + _gpu.BODY

_kernel = None


def _has_metal() -> bool:
    try:
        return bool(mx.metal.is_available())
    except AttributeError:  # pragma: no cover - an MLX built without Metal
        return False


def require() -> None:
    """Refuse, by name, a Metal run on a machine that cannot do one."""
    if not HAVE_MLX:
        raise VectorKernelError(
            "backend='mlx' needs the `mlx` package, which is not installed here: `pip install "
            "mlx` on an Apple silicon Mac. On an NVIDIA GPU use backend='cupy' (`pip install "
            "\"ziplime[cuda]\"`); on any other machine backend='exact', which gives the same "
            "decisions in float64, one combination at a time.")
    if not _has_metal():
        raise VectorKernelError(
            "backend='mlx' runs a Metal kernel, and this MLX has no Metal device -- it is MLX on "
            "CUDA or on the CPU. On an NVIDIA GPU use backend='cupy' (`pip install "
            "\"ziplime[cuda]\"`), which runs the same kernel; elsewhere backend='exact'.")


def _compiled():
    global _kernel
    if _kernel is None:
        _kernel = mx.fast.metal_kernel(
            name="ziplime_vector_batch",
            input_names=list(_gpu.INPUT_NAMES),
            output_names=list(_gpu.OUTPUT_NAMES),
            header=_HEADER,
            source=_SOURCE,
        )
    return _kernel


def _packed_signals(stacks: dict[str, np.ndarray]):
    """Four boolean ``(combinations, T, N)`` stacks as one byte per cell, combination innermost."""
    packed = mx.zeros(stacks["entries"].shape, dtype=mx.uint8)
    for name, intent in _gpu.SIGNAL_BITS:
        # A long-only sweep leaves both short stacks empty; copying them over is most of the
        # transfer for nothing.
        if stacks[name].any():
            packed = packed | (mx.array(stacks[name]).astype(mx.uint8) * (1 << intent))
    return mx.transpose(packed, (1, 2, 0))


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
    inputs = [_packed_signals(stacks)] + [mx.array(host[name]) for name in _gpu.INPUT_NAMES[1:]]

    outputs = _compiled()(
        inputs=inputs,
        grid=(n_combos, 1, 1),
        threadgroup=(min(_gpu.THREADGROUP, n_combos), 1, 1),
        output_shapes=_gpu.output_shapes(n_combos, n_bars, n_columns),
        output_dtypes=[mx.float32, mx.float32, mx.int32, mx.float32],
    )
    mx.eval(*outputs)
    return _gpu.summary(*(np.array(output) for output in outputs))
