"""Instrument utils for cross."""

from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.vectorized_containers import VectorUniverse, Vectorized
from ziplime.sweep.convert import Runtime
from ziplime.sweep.cross.choice import Choice
from ziplime.sweep.cross.cross_section import CrossSection
from ziplime.sweep.cross.names import Names
from ziplime.sweep.cross.ranking_value import Ranking
from ziplime.sweep.cross.selection import Selection
from ziplime.sweep.cross.sweep_dict import SweepDict
from ziplime.sweep.cross.column_utils import _columns
from ziplime.sweep.cross.column_utils import column_index
from ziplime.sweep.cross.ranking_utils import rank


def ranks_instruments(iterable) -> bool:
    return ((isinstance(iterable, SweepDict) and iterable.arrays) or
            (isinstance(iterable, Vectorized) and len(iterable) == 1 and
             (iterable[0] is ALL or isinstance(iterable[0], tuple) and iterable[0][0] is ALL)) or
            _is_universe(iterable))


def _is_universe(iterable) -> bool:
    return isinstance(iterable, VectorUniverse)


def cross_section_of(xp: ModuleType, values, mask):
    """A comprehension over instruments: its values and which instruments it kept."""
    names = _columns()[0]
    if values is ALL or isinstance(values, tuple) and values and values[0] is ALL:
        return Names(xp, xp.logical_or(xp.zeros((1, len(names))) != 0, mask))
    shape = xp.zeros((1, len(names)))
    values = values + shape
    mask = xp.logical_or(xp.zeros(values.shape) != 0, mask)
    return CrossSection(values, mask)


def as_ranking(xp: ModuleType, value):
    """A list of instruments as a Ranking -- its order as places; None if it is not one."""
    n = len(_columns()[0])
    if isinstance(value, Ranking):
        return value
    if isinstance(value, (Selection, Names, Choice)):
        mask = value.mask + xp.zeros((1, n)) != 0 if is_array(value.mask) or value.mask is not True \
            else xp.ones((1, n)) != 0
        places = xp.cumsum(mask.astype(xp.int32), axis=1) - 1
        return Ranking(xp, places, mask)
    if isinstance(value, (list, tuple)):
        columns = [column_index(item) for item in value]
        if any(column is None for column in columns) or len(set(columns)) != len(columns):
            return None
        places, present = np.zeros((1, n)), np.zeros((1, n), dtype=bool)
        for position, column in enumerate(columns):
            places[0, column], present[0, column] = position, True
        return Ranking(xp, xp.array(places), xp.array(present),
                       {column: item for column, item in zip(columns, value)})
    return None


def _instrument_list(value) -> bool:
    return isinstance(value, (Ranking, Selection, Names, Choice)) or \
        (isinstance(value, (list, tuple)) and all(column_index(item) is not None for item in value))


def merge_instruments(xp: ModuleType, mask, new, previous):
    """``new`` where the mask holds and ``previous`` elsewhere, for lists of instruments: a list
    that differs between combinations. None if they are not both lists of instruments."""
    if not (_instrument_list(new) and _instrument_list(previous)):
        return None
    if not any(isinstance(v, (Ranking, Selection, Names, Choice)) or v for v in (new, previous)):
        return None  # two empty lists
    new, previous = as_ranking(xp, new), as_ranking(xp, previous)
    if new is None or previous is None:
        return None
    return Ranking(xp, xp.where(mask, new.places, previous.places),
                   xp.where(mask, new.present, previous.present),
                   {**previous.items, **new.items})


def sort_instruments(runtime: Runtime, items, key: Callable | None, reverse: bool):
    """``sorted(items, key=key)`` / ``items.sort(key=key)`` for a list of instruments whose key
    differs between combinations: the key of every instrument, ranked per combination. None when
    plain Python does it: not instruments, or keys that are plain numbers."""
    from ziplime.sweep.lazy import is_lazy, value_of

    xp = runtime.xp
    if isinstance(items, SweepDict) and not items.arrays:
        items = list(dict.keys(items))
    if not _instrument_list(items) or key is None:
        return None
    ranking = as_ranking(xp, items)
    if ranking is None:
        return None
    names = _columns()[0]
    candidates = sorted(ranking.items) if ranking.items else \
        [c for c in range(len(names))]
    scores, plain = {}, True
    for column in candidates:
        item = ranking.items.get(column, names[column])
        score = key(item)
        if is_lazy(score):
            score = value_of(score)
        plain = plain and not is_array(score)
        scores[column] = score
    if plain and isinstance(items, (list, tuple)):
        return None
    n = len(names)
    values = xp.zeros((1, n)) + runtime.nan
    for column, score in scores.items():
        here = np.zeros((1, n), dtype=bool)
        here[0, column] = True
        values = xp.where(xp.array(here), score, values)
    present = xp.logical_or(xp.zeros(values.shape) != 0, ranking.present)
    values = values + xp.zeros(present.shape)
    order = ranking.places + xp.zeros(values.shape)
    places = rank(xp, values, present, descending=reverse, inf=runtime.inf, order=order)
    return Ranking(xp, places, present, dict(ranking.items))
