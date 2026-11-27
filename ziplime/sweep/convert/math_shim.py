"""Math shim for convert."""

import math
from types import ModuleType
from ziplime.sweep.array_utils import is_array


class _MathShim:
    """``math`` that accepts arrays: math.isnan(signals["close"][name])."""

    def __init__(self, xp: ModuleType) -> None:
        self._xp = xp

    def __getattr__(self, name: str):
        original = getattr(math, name)
        vector = getattr(self._xp, {"fabs": "abs"}.get(name, name), None)
        if not callable(original) or vector is None:
            return original

        def function(*args):
            if any(map(is_array, args)):
                return vector(*args)
            return original(*args)
        return function
