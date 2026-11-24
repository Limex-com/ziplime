"""Price utils for api."""

from types import ModuleType
import numpy as np
from ziplime.sweep.array_utils import is_array


def _consistent_round(xp: ModuleType, value):
    """Half away from zero, as ziplime's consistent_round."""
    return xp.sign(value) * xp.floor(abs(value) + 0.5)


def asymmetric_round(xp: ModuleType, price, prefer_round_down, tick: float = 0.01):
    """finance.execution.asymmetric_round_price for arrays: buy limits round down, sell limits up,
    stops the other way."""
    diff = (0.95 - 0.5) * tick - np.finfo(float).eps * 10
    shifted = xp.where(prefer_round_down, price - diff, price + diff)
    rounded = tick * _consistent_round(xp, shifted / tick)
    return xp.where(abs(rounded) < 1e-12, 0.0, rounded)


def numeric(value) -> bool:
    return isinstance(value, (bool, int, float, np.number)) or is_array(value)
