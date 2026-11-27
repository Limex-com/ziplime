"""Groups for frames."""

from ziplime.sweep.errors import NotConvertible
from ziplime.sweep import frames
from ziplime.sweep.frames.col import Col
from ziplime.sweep.frames.history import History
from ziplime.sweep.frames.statistic import Statistic
from ziplime.sweep.frames.validation import _refuse


class Groups:
    """``frame.group_by("sid")``."""

    def __init__(self, frame: History) -> None:
        self._frame = frame

    def agg(self, *expressions, **named) -> 'frames.Table':
        from ziplime.sweep.frames.table import Table

        statistics = []
        for expression in expressions:
            statistics += list(expression) if isinstance(expression, (list, tuple)) else [expression]
        statistics += [expression.alias(name) if isinstance(expression, Statistic) else expression
                       for name, expression in named.items()]
        if not statistics or not all(isinstance(each, Statistic) for each in statistics):
            raise NotConvertible("group_by('sid').agg(...) takes aggregations of pl.col(...) in a "
                                 "sweep: .mean(), .sum(), .std(), .var(), .min(), .max(), "
                                 ".median(), .first(), .last(), .count(), .len().")
        return Table(self._frame, statistics)

    def _all_fields(self, op: str) -> 'frames.Table':
        from ziplime.sweep.frames.table import Table

        fields = self._frame._fields
        if not fields:
            raise NotConvertible(f"group_by('sid').{op}() needs the fields asked for: "
                                 f"data.history(..., fields=[...]).")
        return Table(self._frame, [Col(field)._statistic(op) for field in fields])

    def mean(self):
        return self._all_fields("mean")

    def sum(self):
        return self._all_fields("sum")

    def std(self):
        return self._all_fields("std")

    def var(self):
        return self._all_fields("var")

    def min(self):
        return self._all_fields("min")

    def max(self):
        return self._all_fields("max")

    def median(self):
        return self._all_fields("median")

    def first(self):
        return self._all_fields("first")

    def last(self):
        return self._all_fields("last")

    def count(self):
        return self._all_fields("count")

    def __getattr__(self, name: str):
        if name.startswith("__") or name.startswith("sweep_"):
            raise AttributeError(name)
        _refuse(f"group_by('sid').{name}")
