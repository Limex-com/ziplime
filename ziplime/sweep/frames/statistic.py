"""Statistic for frames."""

import operator
from collections.abc import Callable
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.frames.validation import _refuse


class Statistic:
    """An aggregation in ``agg(...)``: its output name and how to compute it from the windows.
    ``compute(read)`` with ``read(field, op)`` -> (combinations × instruments)."""

    def __init__(self, name: str, compute: Callable, field: str) -> None:
        self.name, self.compute, self.field = name, compute, field

    def alias(self, name: str) -> 'Statistic':
        return Statistic(name, self.compute, self.field)

    def _combine(self, other: 'Statistic | int | float', op: Callable, reflected: bool = False) -> 'Statistic':
        if isinstance(other, Statistic):
            right = other.compute
        elif isinstance(other, (int, float)) and not isinstance(other, bool):
            right = lambda read: other  # noqa: E731
        else:
            raise NotConvertible("An aggregation combines with numbers and other aggregations "
                                 "only in a sweep.")
        left = self.compute
        if reflected:
            # polars names `1 - pl.col("x").mean()` after its left side: "literal"
            name = other.name if isinstance(other, Statistic) else "literal"
            return Statistic(name, lambda read: op(right(read), left(read)), self.field)
        return Statistic(self.name, lambda read: op(left(read), right(read)), self.field)

    def __add__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.add)

    def __radd__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.add, reflected=True)

    def __sub__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.sub)

    def __rsub__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.sub, reflected=True)

    def __mul__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.mul)

    def __rmul__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.mul, reflected=True)

    def __truediv__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.truediv)

    def __rtruediv__(self, other: 'Statistic | int | float'):
        return self._combine(other, operator.truediv, reflected=True)

    def __neg__(self) -> 'Statistic':
        compute = self.compute
        return Statistic(self.name, lambda read: -compute(read), self.field)

    def __getattr__(self, name: str):
        if name.startswith("__"):
            raise AttributeError(name)
        _refuse(f"pl.col(...).<aggregation>().{name}")
