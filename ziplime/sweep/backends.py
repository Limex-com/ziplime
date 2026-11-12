"""Which array library runs the sweep.

The simulation is written against a numpy-like module ``xp``; the backend picks it:

    mlx    Apple GPU through Metal, float32 (NVIDIA too, with ``mlx[cuda]`` on Linux)
    cupy   NVIDIA GPU through CUDA, or AMD through CuPy built for ROCm, float32
    dpnp   Intel GPU (Arc) through SYCL, float32
    numpy  CPU, float64 -- the exact recomputation, and ``backend: cpu`` across processes

MLX, CuPy and dpnp are optional: ``pip install "ziplime[mlx]"`` or ``"ziplime[cuda]"``; dpnp comes
from Intel's index (see docs/backtesting/sweep-performance.md).
"""
import os

import numpy as np

from ziplime.sweep.params import SweepError


class Backend:
    def __init__(self, name: str):
        self.name = name
        if name == "mlx":
            try:
                import mlx.core as mx
            except ImportError:
                raise SweepError('backend "mlx" needs MLX: pip install "ziplime[mlx]" (Apple '
                                 'silicon), or "mlx[cuda]" on Linux with an NVIDIA GPU.') from None
            self.xp, self.float, self.bool = mx, np.float32, mx.bool_
            if not self.metal and hasattr(mx, "set_cache_limit"):
                # On CUDA freed buffers stay in MLX's cache, in the card's own memory: a long run
                # of bars fills 24 GB with them unless the cache is bounded
                mx.set_cache_limit(int(os.environ.get("ZIPLIME_SWEEP_CACHE_LIMIT", 2 * 1024 ** 3)))
        elif name == "cupy":
            try:
                import cupy as cp
            except ImportError:
                raise SweepError('backend "cupy" needs CuPy: pip install "ziplime[cuda]" (NVIDIA), '
                                 'or CuPy built for ROCm (AMD, Linux).') from None
            self.xp, self.float, self.bool = cp, np.float32, cp.bool_
        elif name == "dpnp":
            try:
                import dpnp
            except ImportError:
                raise SweepError('backend "dpnp" needs dpnp: pip install --index-url '
                                 'https://software.repos.intel.com/python/pypi dpnp (Linux or '
                                 'Windows, with an Intel GPU).') from None
            # float32 whatever the device offers: Arc GPUs have no double precision, and every GPU
            # backend ranks in the same precision
            self.xp, self.float, self.bool = dpnp, np.float32, dpnp.bool
        elif name == "numpy":
            self.xp, self.float, self.bool = np, np.float64, np.bool_
        else:
            raise SweepError(f"Unknown backend {name!r}.")

    @property
    def lazy(self) -> bool:
        """MLX builds a graph and runs it on evaluate; the others run each operation at once."""
        return self.name == "mlx"

    def array(self, values):
        values = np.asarray(values)
        if values.dtype != np.bool_:
            values = values.astype(self.float)
        return self.xp.array(values)

    def scalar(self, value):
        """A Python number as a 0-d array of the right kind, for state that travels through a
        compiled step."""
        if isinstance(value, bool):
            return self.xp.array(value)
        if isinstance(value, int):
            return self.xp.array(value, dtype=self.xp.int32 if self.name == "mlx" else np.int64)
        return self.xp.array(value, dtype=self.xp.float32 if self.name != "numpy" else np.float64)

    def evaluate(self, *arrays):
        if self.name == "mlx":
            self.xp.eval(*arrays)

    def numpy(self, values) -> np.ndarray:
        """Back to host memory (CuPy and dpnp refuse np.array)."""
        if self.name == "cupy":
            return values.get()
        if self.name == "dpnp":
            return self.xp.asnumpy(values)
        return np.asarray(values)

    def compile(self, function):
        """The step compiled into a few kernels. Not shapeless: on CUDA that fails inside MLX
        (an illegal memory access) and on Apple GPUs it is slower; ZIPLIME_SWEEP_SHAPELESS=1 to try."""
        if self.name != "mlx":
            return function
        if os.environ.get("ZIPLIME_SWEEP_SHAPELESS") == "1":
            return self.xp.compile(function, shapeless=True)
        return self.xp.compile(function)

    @property
    def metal(self) -> bool:
        """MLX on an Apple GPU, with unified memory; False for MLX on CUDA and everything else."""
        if self.name != "mlx":
            return False
        try:
            return bool(self.xp.metal.is_available())
        except AttributeError:
            return False

    @property
    def device_memory(self) -> bool:
        """The arrays live in a GPU's own memory: MLX on CUDA, CuPy, dpnp."""
        return self.name in ("cupy", "dpnp") or (self.name == "mlx" and not self.metal)
