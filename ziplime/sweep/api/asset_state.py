"""Asset state for api."""

import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep import api
from ziplime.sweep.api.columns import Columns


class AssetState:
    """``context.entry = {}`` -- a dict keyed by instrument -- for every combination: the values and
    which keys are present, each an array (combinations × instruments).

    Writes, deletions and ``in`` act where the statement's mask holds, as an ``if`` in the strategy
    would decide them; a loop over its items runs for the instruments it holds.
    """

    def __init__(self, initial: dict, columns: Columns, context: 'api.Context') -> None:
        self._columns, self._context = columns, context
        xp = columns.backend.xp
        values = np.full((1, len(columns.names)), np.nan)
        present = np.zeros((1, len(columns.names)), dtype=bool)
        for key, item in initial.items():
            values[0, columns.index(key)] = float(item)
            present[0, columns.index(key)] = True
        self._v, self._p = columns.backend.array(values), xp.array(present)

    @property
    def _xp(self):
        return self._columns.backend.xp

    def _where(self, key):
        runtime = self._context._runtime
        mask = runtime.mask if runtime is not None else True
        if runtime is None:
            return self._columns.only(key)
        return runtime.and_(mask, lambda: self._columns.only(key))

    # what the compiled step carries
    def dump(self, like):
        return self._v + like * 0, self._xp.logical_or(self._p, like != like)

    def load(self, values, present) -> None:
        self._v, self._p = values, present

    # the dict's behaviour
    def __getitem__(self, key):
        return self._columns.select(self._v, key)

    def __setitem__(self, key, value) -> None:
        where = self._where(key)
        xp = self._xp
        self._v = xp.where(where, value, self._v) if where is not True \
            else value + xp.zeros(self._v.shape)
        self._p = xp.logical_or(self._p, where) if where is not True else \
            xp.logical_or(self._p, True)

    def get(self, key, default=None):
        runtime = self._context._runtime
        fallback = (runtime.nan if runtime is not None else np.nan) if default is None else default
        return self._xp.where(self._columns.select(self._p, key),
                              self._columns.select(self._v, key), fallback)

    def sweep_delete(self, key) -> None:
        where = self._where(key)
        self._p = self._xp.logical_and(self._p, self._xp.logical_not(where)) \
            if where is not True else self._p & False

    __delitem__ = sweep_delete

    def pop(self, key, default=None):
        value = self.get(key, default)
        self.sweep_delete(key)
        return value

    def clear(self) -> None:
        self.sweep_delete(ALL)

    def sweep_contains(self, key):
        return self._columns.select(self._p, key)

    def sweep_len(self):
        return self._xp.sum(self._p, axis=1, keepdims=True)

    def items(self):
        return Vectorized([(ALL, self._v)], mask=self._p)

    def keys(self):
        return Vectorized([ALL], mask=self._p)

    def values(self):
        return Vectorized([self._v], mask=self._p)

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        raise NotConvertible("len(context.dict) differs between combinations; it is supported in "
                             "converted code, where it is a number per combination.")
