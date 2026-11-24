"""Select utils for lazy."""

import operator
from collections.abc import Callable
from ziplime.sweep.lazy.select import Select


def _distribute(function: Callable, select: Select) -> Select:
    return Select(select.mask, function(select.yes), function(select.no))


def _arith(name: str):
    def method(self, other):
        return _distribute(lambda value: getattr(operator, name)(value, other), self)

    def reflected(self, other):
        return _distribute(lambda value: getattr(operator, name)(other, value), self)
    return method, reflected
