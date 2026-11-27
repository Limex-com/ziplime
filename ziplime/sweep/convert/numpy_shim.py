"""Numpy shim for convert."""

from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.support import NUMPY_CONSTANTS
from ziplime.sweep.convert.support import NUMPY_ELEMENTWISE
from ziplime.sweep.convert.support import NUMPY_REDUCTIONS
from ziplime.sweep.convert.reduction_utils import _reduce


class _NumpyShim:
    """``np`` inside a converted strategy: its functions on the backend's arrays."""

    def __init__(self, xp: ModuleType) -> None:
        self._xp = xp

    def __getattr__(self, name: str):
        original = getattr(np, name)
        if name in NUMPY_CONSTANTS or not callable(original):
            return original
        return _HistoryAware(original, self._array_function(name, original), f"np.{name}")

    def _array_function(self, name: str, original: Callable):
        xp = self._xp
        if name == "trunc":
            return lambda value: (xp.sign(value) * xp.floor(abs(value)) if is_array(value)
                                  else np.trunc(value))
        if name in NUMPY_ELEMENTWISE:
            vector = getattr(xp, NUMPY_ELEMENTWISE[name])

            def elementwise(*args, **kwargs):
                if any(map(is_array, args)):
                    return vector(*args, **kwargs)
                return original(*args, **kwargs)
            return elementwise
        if name in NUMPY_REDUCTIONS:
            return lambda values, *args, **kwargs: _reduce(xp, name, original, values, args, kwargs)

        def unsupported(*args, **kwargs):
            raise NotConvertible(f"np.{name} has no counterpart in a sweep on values that differ "
                                 f"between combinations; the supported NumPy functions are "
                                 f"elementwise ones and reductions over a list -- or any of them "
                                 f"on an array of history (data.history(...)['close'].to_numpy()).")
        return unsupported


class _HistoryAware:
    """A function of NumPy or of another library inside converted code: on history rows
    (lazy.py) it is recorded, to run on the host with the real rows; otherwise ``plain``.
    ``np.maximum.accumulate`` and the other attributes of a ufunc are recorded the same way."""

    def __init__(self, original: Callable, plain: Callable, name: str) -> None:
        self._original, self._plain, self._name = original, plain, name

    def __call__(self, *args, **kwargs):
        from ziplime.sweep.lazy import contains_lazy, record
        if contains_lazy(args, kwargs):
            return record(self._original, args, kwargs)
        return self._plain(*args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        attribute = getattr(self._original, name)
        if not callable(attribute):
            return attribute

        def plain(*args, **kwargs):
            raise NotConvertible(f"{self._name}.{name} runs on an array of history only in a "
                                 f"sweep.")
        return _HistoryAware(attribute, plain, f"{self._name}.{name}")
