"""Sweep dict for cross."""

from collections.abc import Callable
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.convert import Runtime
from ziplime.sweep.cross.cross_section import CrossSection
from ziplime.sweep.cross.column_utils import _columns
from ziplime.sweep.cross.column_utils import _numeric
from ziplime.sweep.cross.column_utils import _select
from ziplime.sweep.cross.column_utils import _windows_of
from ziplime.sweep.cross.column_utils import column_index
from ziplime.sweep.cross.column_utils import column_mask


class SweepDict(dict):
    """A dict that holds the instruments' entries as arrays.

    Until a key that is an instrument (or the loop variable standing for all of them) is written it
    is an ordinary dict.
    """

    def __init__(self, runtime: Runtime, *args, **kwargs) -> None:
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

    def _write(self, key, value, where) -> None:
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

    def scores(self, key: Callable | None = None):
        """The values to rank by, and which instruments take part."""
        if key is None:
            raise NotConvertible("sorted() of instruments needs a key -- the score to rank by.")
        return key(ALL), self._present
