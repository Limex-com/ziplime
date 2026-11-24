"""Col for frames."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.frames.support import AGGREGATIONS
from ziplime.sweep.frames.pick import Pick
from ziplime.sweep.frames.statistic import Statistic
from ziplime.sweep.frames.validation import _refuse


class Col:
    """``pl.col(name)``: compared on ``sid``, it picks instruments; aggregated, a statistic."""

    def __init__(self, name: str) -> None:
        self.name = name

    def _sid(self) -> None:
        if self.name != "sid":
            raise NotConvertible(f"A history frame is filtered by sid in a sweep, not by "
                                 f"{self.name!r}.")

    def __eq__(self, other) -> 'Pick':
        self._sid()
        return Pick([other])

    def is_in(self, values) -> 'Pick':
        self._sid()
        return Pick(list(values))

    __hash__ = object.__hash__

    def _statistic(self, op: str, ddof: int = 1) -> 'Statistic':
        if self.name in ("sid", "date"):
            raise NotConvertible(f"pl.col({self.name!r}).{op}() is not supported in a sweep.")
        if ddof != 1:
            raise NotConvertible(f"pl.col(...).{op}() is supported with ddof=1 only.")
        field, statistic = self.name, AGGREGATIONS[op]
        return Statistic(field, lambda read: read(field, statistic), field)

    def mean(self):
        return self._statistic("mean")

    def sum(self):
        return self._statistic("sum")

    def std(self, ddof: int = 1):
        return self._statistic("std", ddof)

    def var(self, ddof: int = 1):
        return self._statistic("var", ddof)

    def min(self):
        return self._statistic("min")

    def max(self):
        return self._statistic("max")

    def median(self):
        return self._statistic("median")

    def first(self):
        return self._statistic("first")

    def last(self):
        return self._statistic("last")

    def count(self):
        return self._statistic("count")

    def len(self):
        return self._statistic("len")

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.col(...).{name}")
