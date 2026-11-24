"""Reduction utils for convert."""

from types import ModuleType
import numpy as np
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.support import _RUNTIME_INF


def _reduce(xp: ModuleType, name: str, original, values, args, kwargs):
    if args or kwargs.get("axis") is not None:
        raise NotConvertible(f"np.{name} with an axis reduces over combinations in a sweep, not "
                             f"over what the strategy meant; reduce a list: np.{name}([a, b]).")
    ddof = kwargs.get("ddof", 0)
    if isinstance(values, (list, tuple)):
        if not any(map(is_array, values)):
            return original(values, **kwargs)
        shape = np.broadcast_shapes(*[getattr(v, "shape", ()) for v in values])
        base = xp.zeros(shape)
        stacked = xp.stack([v + base for v in values])  # (values, combinations, instruments)
        count = len(values)
        present = xp.logical_not(xp.isnan(stacked))
        clean = xp.where(present, stacked, 0.0)
        if name in ("mean", "sum", "prod", "min", "max", "amin", "amax", "median"):
            target = {"amin": "min", "amax": "max"}.get(name, name)
            return getattr(xp, target)(stacked, axis=0)
        if name in ("std", "var"):
            mean = xp.mean(stacked, axis=0)
            var = xp.sum((stacked - mean) ** 2, axis=0) / max(count - ddof, 1)
            return xp.sqrt(var) if name == "std" else var
        if name == "nansum":
            return xp.sum(clean, axis=0)
        if name == "nanmean":
            return xp.sum(clean, axis=0) / xp.sum(present, axis=0)
        if name in ("nanmin", "nanmax"):
            fill = _RUNTIME_INF[0] if name == "nanmin" else -_RUNTIME_INF[0]
            reduce = xp.min if name == "nanmin" else xp.max
            return reduce(xp.where(present, stacked, fill), axis=0)
    if is_array(values):
        # One number per instrument in the strategy: a reduction of one number is the number
        return values * 0.0 if name in ("std", "var") else values
    return original(values, **kwargs)
