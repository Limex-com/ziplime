"""Ranking utils for cross."""

from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert import Runtime
from ziplime.sweep.cross.support import PAIRWISE
from ziplime.sweep.cross.ranking_value import Ranking
from ziplime.sweep.cross.sweep_dict import SweepDict
from ziplime.sweep.cross.column_utils import _columns


def rank(xp: ModuleType, scores, present, descending: bool, inf=np.inf, order=None):
    """Each instrument's place in Python's sorted() of its combination's present instruments:
    those ahead of it are the better ones and the equal ones inserted before it (sorted is stable,
    with reverse=True too). NaN ranks last; absent instruments take no place. ``order``: where
    each instrument stood before sorting, when not in the universe's order."""
    n = scores.shape[-1]
    worst = -inf if descending else inf
    scores = xp.where(xp.isnan(scores), worst, scores)
    if n <= PAIRWISE:
        mine, theirs = scores[:, :, None], scores[:, None, :]
        better = (theirs > mine) if descending else (theirs < mine)
        if order is None:
            index = np.arange(n)
            earlier = xp.array(index[None, :] < index[:, None])[None]  # [i, j]: j before i
        else:
            earlier = order[:, None, :] < order[:, :, None]
        ahead = xp.logical_or(better, xp.logical_and(theirs == mine, earlier))
        ahead = xp.logical_and(ahead, present[:, None, :])
        return xp.sum(ahead, axis=2)
    scores = xp.where(present, scores, worst)
    order = xp.argsort(-scores if descending else scores, axis=1)
    return xp.argsort(order, axis=1)


def ranking(
    xp: ModuleType,
    iterable,
    key: Callable | None,
    reverse: bool,
    runtime: Runtime | None = None,
) -> Ranking:
    """``sorted(iterable, key=key, reverse=reverse)`` of instruments."""
    if key is None:
        raise NotConvertible("sorted() of instruments needs a key -- the score to rank by.")
    if isinstance(iterable, SweepDict) and iterable.arrays:
        scores, present = iterable.scores(key)
    else:
        scores, present = key(ALL), getattr(iterable, "mask", True)
    shape_like = xp.zeros((1, len(_columns()[0])))
    scores = scores + shape_like
    present = xp.logical_or(xp.zeros(scores.shape) != 0, present)
    inf = runtime.inf if runtime is not None else np.inf
    return Ranking(xp, rank(xp, scores, present, descending=reverse, inf=inf), present)


def reduce_across(xp: ModuleType, name: str, values, runtime: Runtime | None = None):
    """``sum``/``max``/``min``/``any``/``all``/``len`` of a cross section: across each
    combination's present instruments."""
    present = values.present
    if name == "sum":
        return xp.sum(xp.where(present, values.values, 0.0), axis=1, keepdims=True)
    if name in ("any", "all"):
        truth = values.values if values.values.dtype == getattr(xp, "bool_", np.bool_) \
            else values.values != 0
        if name == "any":
            return xp.any(xp.logical_and(present, truth), axis=1, keepdims=True)
        return xp.all(xp.logical_or(xp.logical_not(present), truth), axis=1, keepdims=True)
    inf = runtime.inf if runtime is not None else np.inf
    fill = -inf if name == "max" else inf
    reduce = xp.max if name == "max" else xp.min
    return reduce(xp.where(present, values.values, fill), axis=1, keepdims=True)
