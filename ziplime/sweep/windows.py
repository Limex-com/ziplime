"""History windows prepared on the host and shared with sweep strategies."""

from collections.abc import Iterable

import numpy as np

from ziplime.sweep.array_utils import is_array
from ziplime.sweep.backends import Backend
from ziplime.sweep.errors import NotConvertible, Rediscover
from ziplime.sweep.market import WINDOW_OPS, Market, daily_window_values, window_values


class Windows:
    """data.history statistics: computed on the host per distinct bar_count, read per bar."""

    def __init__(self, market: Market, backend: Backend) -> None:
        self.market, self.backend = market, backend
        #: (field, op, before) -> sorted distinct lengths
        self.lengths = {}
        self.stacks = {}
        self.discovering = False
        self.now = {}
        from ziplime.sweep.lazy import LazyRegistry
        #: The strategy's own computations on history rows, replayed on the host (see lazy.py)
        self.lazy = LazyRegistry(self)
        #: What happened to a compiled run, for the report (explain.py)
        self.notes = set()

    def request(self, field: str, op: str, before: bool, lengths: 'Iterable[int]', daily: bool = False) -> bool:
        key = (field, op, before, daily)
        known = set(self.lengths.get(key, ()))
        if not set(lengths) <= known:
            self.lengths[key] = sorted(known | set(lengths))
            return True
        return False

    def prepare(self) -> None:
        """Host arrays (bars, lengths, instruments) on the device."""
        cache = self.__dict__.setdefault("_host", {})
        for key, lengths in self.lengths.items():
            field, op, before, daily = key
            compute = daily_window_values if daily else window_values
            for length in lengths:
                if (key, length) not in cache:
                    cache[(key, length)] = compute(self.market, field, op, length, before)
            self.stacks[key] = self.backend.array(
                np.stack([cache[(key, length)] for length in lengths], axis=1))
        self.lazy.prepare()

    def at(self, bar: int) -> dict:
        return {key: stack[bar] for key, stack in self.stacks.items()}

    def value(self, field: str, op: str, length, before: bool, daily: bool = False):
        if op not in WINDOW_OPS:
            raise NotConvertible(f"history(...).{op}() is not supported in a sweep.")
        key, xp = (field, op, before, daily), self.backend.xp
        if self.discovering:
            values = (np.unique(self.backend.numpy(length)) if is_array(length)
                      else [length])
            self.request(field, op, before, [int(v) for v in values], daily)
            return xp.zeros((1, len(self.market.columns))) + np.nan
        lengths = self.lengths.get(key)
        if lengths is None:
            self.request(field, op, before, [length] if not is_array(length) else [], daily)
            raise Rediscover()
        rows = self.now[key]  # (lengths, instruments)
        if not is_array(length):
            if length not in lengths:
                self.request(field, op, before, [length], daily)
                raise Rediscover()
            k = lengths.index(int(length))
            return rows[k:k + 1]
        known = self.backend.array(np.array(lengths, dtype=float))
        index = xp.sum(length > known[None, :], axis=1).astype(xp.int32)  # (combinations,)
        return xp.take(rows, index, axis=0)
