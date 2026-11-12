"""Comparing instruments with each other inside a converted strategy.

A rotation picks the best few instruments by a score::

    scores = {}
    for name, asset in context.universe.items():
        scores[name] = context.signals["momentum"][name]
    best = sorted(scores, key=scores.get, reverse=True)[:config.top_k]
    for name, asset in context.universe.items():
        target = weight if name in best else 0.0

In a sweep the loop is one pass with ``name`` standing for every instrument, so ``scores`` gets one
entry, an array (combinations × instruments). :class:`SweepDict` is that dict: a plain dict for any
other key, and for the instruments an array of values with a mask of which are present. Sorting it
ranks the instruments within each combination; ``[:k]``, ``in``, ``len``, ``sum(d.values())`` and
``max(d, key=...)`` are then masks and numbers per combination -- what the same calls give in the
engine, where ``scores`` is an ordinary dict of floats.
"""
import numpy as np

from ziplime.sweep.convert import ALL, NotConvertible, Vectorized, is_array

#: Up to this many instruments ranks are counted pairwise, which is exactly Python's stable sort;
#: above it an argsort, where ties between equal float scores are not expected.
PAIRWISE = 64


def _columns():
    names, listings, array = type(ALL)._columns
    return names, listings, array


def column_index(key):
    """The column of an instrument key (name, listing or sid); None for anything else."""
    if key is ALL or isinstance(key, bool) or is_array(key):
        return None
    names, listings, _ = _columns()
    for k, (name, listing) in enumerate(zip(names, listings)):
        try:
            if key == name or key == listing or key == getattr(listing, "sid", None):
                return k
        except Exception:
            return None
    return None


def column_mask(key):
    """(1 × instruments): True at this key -- every instrument for the loop variable."""
    names, listings, array = _columns()
    if key is ALL:
        return True
    hits = [k for k, (name, listing) in enumerate(zip(names, listings))
            if key == name or key == listing or key == getattr(listing, "sid", None)]
    if not hits:
        return None
    mask = np.zeros((1, len(names)), dtype=bool)
    mask[0, hits[0]] = True
    return array(mask)


class SweepDict(dict):
    """A dict that holds the instruments' entries as arrays.

    Until a key that is an instrument (or the loop variable standing for all of them) is written it
    is an ordinary dict.
    """

    def __init__(self, runtime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._runtime = runtime
        self._values = None
        self._present = None
        for key in [key for key in dict.keys(self)
                    if column_mask(key) is not None and _numeric(dict.__getitem__(self, key))]:
            value = dict.pop(self, key)
            self._write(key, value, where=True)

    @property
    def _xp(self):
        return self._runtime.xp

    @property
    def arrays(self) -> bool:
        return self._values is not None

    def _write(self, key, value, where):
        xp = self._xp
        names = _columns()[0]
        if self._values is None:
            self._values = xp.zeros((1, len(names))) + self._runtime.nan
            self._present = xp.zeros((1, len(names))) != 0
        here = column_mask(key)
        if where is not True:
            here = here if here is True else xp.logical_and(here, where)
            here = where if here is True else here
        if here is True:
            self._values = value + xp.zeros(self._values.shape)
            self._present = xp.logical_or(self._present, True)
        else:
            self._values = xp.where(here, value, self._values)
            self._present = xp.logical_or(self._present, here)

    def __setitem__(self, key, value):
        from ziplime.sweep.lazy import is_lazy, value_of
        if is_lazy(value):
            # A number computed from history is a number; history rows stay what they are
            leaf_windows = _windows_of(value)
            if leaf_windows is not None and leaf_windows.lazy.is_number(value):
                value = value_of(value)
        if column_mask(key) is None or not _numeric(value):
            if key is ALL:
                raise NotConvertible("A dict keyed by the loop's instrument holds numbers in a "
                                     "sweep.")
            return dict.__setitem__(self, key, value)
        self._write(key, value, self._runtime.mask)

    def __getitem__(self, key):
        if key is ALL and not self.arrays:
            return self._every_instrument()
        if column_mask(key) is None or not self.arrays:
            return dict.__getitem__(self, key)
        return _select(self._values, key)

    def _every_instrument(self):
        """``d[name]`` for every instrument at once, from a plain dict: a dict of the
        instruments themselves (``{s: context.symbol(s) ...}``) gives the instrument."""
        names, listings, _ = _columns()
        by_column = {column_index(key): value for key, value in dict.items(self)}
        if any(column not in by_column for column in range(len(listings))):
            raise KeyError(ALL)  # not every instrument has an entry: nothing to read for all
        for column, listing in enumerate(listings):
            value = by_column[column]
            if not (value is listing or value == listing):
                raise NotConvertible("A dict read with the loop's instrument holds numbers or the "
                                     "instruments themselves in a sweep.")
        return ALL

    def get(self, key, default=None):
        if column_mask(key) is None or not self.arrays:
            return dict.get(self, key, default)
        fallback = self._runtime.nan if default is None else default
        return self._xp.where(_select(self._present, key), _select(self._values, key), fallback)

    def sweep_contains(self, key):
        if column_mask(key) is None or not self.arrays:
            return dict.__contains__(self, key)
        return _select(self._present, key)

    def sweep_delete(self, key):
        if column_mask(key) is None or not self.arrays:
            return dict.__delitem__(self, key)
        where = self._runtime.and_(self._runtime.mask, lambda: column_mask(key))
        self._present = self._xp.logical_and(self._present, self._xp.logical_not(where)) \
            if where is not True else self._present & False

    def pop(self, key, *default):
        if column_mask(key) is None or not self.arrays:
            return dict.pop(self, key, *default)
        value = self.get(key, default[0] if default else None)
        self.sweep_delete(key)
        return value

    def sweep_len(self):
        if not self.arrays:
            return dict.__len__(self)
        return self._xp.sum(self._present, axis=1, keepdims=True) + dict.__len__(self)

    def keys(self):
        return Vectorized([ALL], mask=self._present) if self.arrays else dict.keys(self)

    def sweep_vectorized(self):
        return self.keys() if self.arrays else self

    def __iter__(self):
        return iter(self.keys())

    def items(self):
        return Vectorized([(ALL, self._values)], mask=self._present) if self.arrays else dict.items(self)

    def values(self):
        if not self.arrays:
            return dict.values(self)
        return CrossSection(self._values, self._present)

    def scores(self, key=None):
        """The values to rank by, and which instruments take part."""
        if key is None:
            raise NotConvertible("sorted() of instruments needs a key -- the score to rank by.")
        return key(ALL), self._present


def _numeric(value) -> bool:
    return isinstance(value, (int, float, np.number)) or is_array(value)


def _windows_of(value):
    from ziplime.sweep.lazy import _any_leaf
    try:
        return _any_leaf(value).data._windows
    except NotConvertible:
        return None


def _select(values, key):
    if key is ALL:
        return values
    names, listings, _ = _columns()
    for k, (name, listing) in enumerate(zip(names, listings)):
        if key == name or key == listing or key == getattr(listing, "sid", None):
            return values[:, k:k + 1]
    raise KeyError(key)


class CrossSection(Vectorized):
    """``d.values()`` of a SweepDict, or a comprehension over instruments: what ``sum``, ``max``,
    ``min``, ``any``, ``all`` and ``len`` reduce across the instruments of each combination."""

    def __init__(self, values, present):
        super().__init__([values], mask=present)
        self.values, self.present = values, present

    def sweep_len(self):
        return self.present.sum(axis=1, keepdims=True)


class Ranking:
    """``sorted(...)`` of instruments: each instrument's place within its combination -- and a
    list of instruments that differs between combinations. ``items``: {column: the object the
    strategy put in the list for it (its name or its listing)}."""

    def __init__(self, xp, places, present, items=None):
        self.xp, self.places, self.present = xp, places, present
        self.items = items or {}

    def _count(self):
        return self.xp.sum(self.present, axis=1, keepdims=True)

    def __getitem__(self, item):
        xp, count = self.xp, self._count()

        def place(value, default):
            if value is None:
                return default
            if not is_array(value) and value < 0:
                return count + value
            return value

        if isinstance(item, slice):
            if item.step not in (None, 1):
                raise NotConvertible("A ranking can be sliced from the top ([:k]) or the bottom "
                                     "([-k:]) only.")
            start, stop = place(item.start, 0), place(item.stop, count)
            chosen = xp.logical_and(xp.logical_and(self.places >= start, self.places < stop),
                                    self.present)
            return Selection(xp, chosen)
        if isinstance(item, int):
            return Choice(xp, xp.logical_and(self.places == place(item, 0), self.present))
        raise NotConvertible("A ranking is indexed by a position or sliced by places.")

    def sweep_vectorized(self):
        return Vectorized([ALL], mask=self.present)

    def __iter__(self):
        return iter(Vectorized([ALL], mask=self.present))

    def sweep_len(self):
        return self._count()

    def sweep_contains(self, key):
        return _select(self.present, key)


class Selection:
    """Some instruments of each combination: ``ranked[:k]``."""

    def __init__(self, xp, mask):
        self.xp, self.mask = xp, mask

    def sweep_contains(self, key):
        return _select(self.mask, key)

    def sweep_len(self):
        return self.xp.sum(self.mask, axis=1, keepdims=True)

    def __iter__(self):
        return iter(Vectorized([ALL], mask=self.mask))

    def sweep_vectorized(self):
        return Vectorized([ALL], mask=self.mask)


class Choice:
    """One instrument of each combination: ``ranked[0]``, ``max(scores, key=...)``."""

    def __init__(self, xp, mask):
        self.xp, self.mask = xp, mask

    def sweep_equals_all(self):
        return self.mask

    def __eq__(self, other):
        return _select(self.mask, other)

    def __ne__(self, other):
        return self.xp.logical_not(self.__eq__(other))

    __hash__ = object.__hash__


def rank(xp, scores, present, descending: bool, inf=np.inf, order=None):
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


def ranking(xp, iterable, key, reverse: bool, runtime=None):
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


def ranks_instruments(iterable) -> bool:
    return ((isinstance(iterable, SweepDict) and iterable.arrays) or
            (isinstance(iterable, Vectorized) and len(iterable) == 1 and
             (iterable[0] is ALL or isinstance(iterable[0], tuple) and iterable[0][0] is ALL)) or
            _is_universe(iterable))


def _is_universe(iterable) -> bool:
    from ziplime.sweep.convert import VectorUniverse
    return isinstance(iterable, VectorUniverse)


class Names(Vectorized):
    """A comprehension over instruments that yields the instruments themselves
    (``[name for name in scores if scores[name] > 0]``): a selection."""

    def __init__(self, xp, mask):
        super().__init__([ALL], mask=mask)
        self.xp = xp

    def sweep_len(self):
        return self.xp.sum(self.mask, axis=1, keepdims=True)

    def sweep_contains(self, key):
        return _select(self.mask, key)


def cross_section_of(xp, values, mask):
    """A comprehension over instruments: its values and which instruments it kept."""
    names = _columns()[0]
    if values is ALL or isinstance(values, tuple) and values and values[0] is ALL:
        return Names(xp, xp.logical_or(xp.zeros((1, len(names))) != 0, mask))
    shape = xp.zeros((1, len(names)))
    values = values + shape
    mask = xp.logical_or(xp.zeros(values.shape) != 0, mask)
    return CrossSection(values, mask)


def reduce_across(xp, name, values, runtime=None):
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


# -- lists of instruments that differ between combinations ----------------------------------------

def as_ranking(xp, value):
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


def merge_instruments(xp, mask, new, previous):
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


def sort_instruments(runtime, items, key, reverse):
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
