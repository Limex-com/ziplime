"""Lazy for lazy."""

import operator
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.lazy.method import _Method
from ziplime.sweep.lazy.support import where_in_strategy


class Lazy:
    """A computation on history rows, recorded: ``node`` is (kind, ...)."""

    __array_priority__ = 1000  # NumPy defers to the operators below

    def __init__(self, node) -> None:
        object.__setattr__(self, "node", node)

    # -- arithmetic is recorded; comparisons are numbers
    def __add__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.add, self, other)

    def __radd__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.add, other, self)

    def __sub__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.sub, self, other)

    def __rsub__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.sub, other, self)

    def __mul__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.mul, self, other)

    def __rmul__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.mul, other, self)

    def __truediv__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.truediv, self, other)

    def __rtruediv__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.truediv, other, self)

    def __floordiv__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.floordiv, self, other)

    def __rfloordiv__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.floordiv, other, self)

    def __mod__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.mod, self, other)

    def __rmod__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.mod, other, self)

    def __pow__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.pow, self, other)

    def __rpow__(self, other):
        from ziplime.sweep.lazy.recording import _binary

        return _binary(operator.pow, other, self)

    def __neg__(self):
        from ziplime.sweep.lazy.recording import record

        return record(operator.neg, (self,))

    def __pos__(self):
        return self

    def __abs__(self):
        from ziplime.sweep.lazy.recording import record

        return record(operator.abs, (self,))

    def __lt__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.lt, self, other)

    def __le__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.le, self, other)

    def __gt__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.gt, self, other)

    def __ge__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.ge, self, other)

    def __eq__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.eq, self, other)

    def __ne__(self, other):
        from ziplime.sweep.lazy.recording import _compare

        return _compare(operator.ne, self, other)

    __hash__ = object.__hash__

    def __getitem__(self, key):
        from ziplime.sweep.lazy.recording import record

        return record(operator.getitem, (self, key))

    def __getattr__(self, name: str) -> 'Lazy':
        if name.startswith("__") or name.startswith("sweep_") or name in (
            "shape", "dtype", "ndim", "mask", "_values",
        ):
            raise AttributeError(name)
        return Lazy(("attr", self, name))

    def __call__(self, *args, **kwargs):
        from ziplime.sweep.lazy.recording import record

        kind = self.node[0]
        if kind != "attr":
            raise NotConvertible(f"{where_in_strategy()}: a value computed from history is not "
                                 f"callable.")
        _, owner, name = self.node
        return record(_Method(name), (owner, *args), kwargs)

    def __bool__(self):
        raise NotConvertible(f"{where_in_strategy()}: an array of history is used as True/False "
                             f"here; compare a number computed from it.")

    def __len__(self):
        raise NotConvertible(f"{where_in_strategy()}: len() of history here; use len(...) in the "
                             f"strategy's own code, which the sweep converts.")

    def __iter__(self):
        raise NotConvertible(f"{where_in_strategy()}: iterating an array of history in Python is "
                             f"not supported in a sweep; compute with NumPy on it instead.")

    def __float__(self):
        raise NotConvertible(f"{where_in_strategy()}: float() of history here.")

    def __repr__(self) -> str:
        return f"<history computation {self.node[0]}>"
