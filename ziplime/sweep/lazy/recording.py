"""Recording for lazy."""

from collections.abc import Callable
import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.api import Data
from ziplime.sweep.lazy.lazy import Lazy
from ziplime.sweep.lazy.values import Leaf
from ziplime.sweep.lazy.values import Param
from ziplime.sweep.lazy.select import Select
from ziplime.sweep.lazy.support import where_in_strategy


def is_lazy(value) -> bool:
    return isinstance(value, (Lazy, Select))


def contains_lazy(*values) -> bool:
    for value in values:
        if is_lazy(value):
            return True
        if isinstance(value, (list, tuple)) and any(contains_lazy(item) for item in value):
            return True
        if isinstance(value, dict) and any(contains_lazy(item) for item in value.values()):
            return True
        if isinstance(value, slice) and contains_lazy(value.start, value.stop, value.step):
            return True
    return False


def record(function: Callable, args, kwargs=None):
    """``function(*args, **kwargs)`` with a record among the arguments: a longer record."""
    kwargs = kwargs or {}
    for k, arg in enumerate(args):
        if isinstance(arg, Select):
            return Select(arg.mask, record(function, args[:k] + (arg.yes,) + args[k + 1:], kwargs),
                          record(function, args[:k] + (arg.no,) + args[k + 1:], kwargs))
    for name, arg in kwargs.items():
        if isinstance(arg, Select):
            return Select(arg.mask, record(function, args, {**kwargs, name: arg.yes}),
                          record(function, args, {**kwargs, name: arg.no}))
    return Lazy(("call", function, tuple(_wrap(arg) for arg in args),
                 tuple(sorted((name, _wrap(arg)) for name, arg in kwargs.items()))))


def _wrap(value):
    """An argument of a record: values that differ between combinations become parameters."""
    if isinstance(value, Lazy) or value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if value is ALL:
        raise NotConvertible(f"{where_in_strategy()}: the loop's instrument is passed into a "
                             f"computation on history; pass its history instead.")
    if isinstance(value, slice):
        return slice(_wrap(value.start), _wrap(value.stop), _wrap(value.step))
    if isinstance(value, (list, tuple)):
        return type(value)(_wrap(item) for item in value)
    if isinstance(value, np.generic):
        return value.item()
    if is_array(value):
        if len(value.shape) != 2 or value.shape[1] != 1:
            raise NotConvertible(f"{where_in_strategy()}: a value that differs between "
                                 f"instruments is passed into a computation on history; a "
                                 f"parameter of it has to be the same for every instrument.")
        return Param(value)
    if callable(value) or isinstance(value, (dict, set, frozenset)):
        return value
    raise NotConvertible(f"{where_in_strategy()}: {type(value).__name__} is passed into a "
                         f"computation on history; numbers, strings and history are.")


def _binary(op: Callable, left, right):
    other = right if is_lazy(left) else left
    if is_array(other) or _is_sweep_value(other):
        # A value of the step (the cash, a config field, a signal) meets the record: the record is
        # a number by now, and the arithmetic is the step's
        return op(value_of(left), value_of(right))
    return record(op, (left, right))


def _is_sweep_value(value) -> bool:
    return hasattr(value, "sweep_vectorized") or hasattr(value, "sweep_len")


def _compare(op: Callable, left, right):
    return op(value_of(left), value_of(right))


def is_object_state(value) -> bool:
    """An object of another library initialize keeps on the context -- a fitted model, a scaler:
    not a number, a container, NumPy, pandas, polars or ziplime."""
    if value is None or isinstance(value, (bool, int, float, str, bytes, list, tuple, dict, set,
                                           frozenset, np.ndarray, np.generic)):
        return False
    top = (type(value).__module__ or "").split(".")[0]
    return top not in ("builtins", "ziplime", "numpy", "pandas", "polars", "datetime",
                       "collections", "decimal", "fractions", "pydantic", "types")


def value_of(value):
    """A record as the step's number: (1 or combinations) × (1 or instruments)."""
    if isinstance(value, Select):
        runtime = _any_leaf(value).data._runtime
        return runtime.where(value.mask, value_of(value.yes), value_of(value.no))
    if not isinstance(value, Lazy):
        return value
    leaf = _any_leaf(value)
    return leaf.data._windows.lazy.value(value)


def _any_leaf(value):
    if isinstance(value, Select):
        for side in (value.yes, value.no):
            if is_lazy(side):
                return _any_leaf(side)
    for leaf in _leaves(value):
        return leaf
    raise NotConvertible("A computation on history without history in it.")


def _leaves(value):
    if isinstance(value, Leaf):
        yield value
    elif isinstance(value, Lazy):
        node = value.node
        if node[0] == "leaf":
            yield node[1]
        elif node[0] == "attr":
            yield from _leaves(node[1])
        else:
            for arg in node[2]:
                yield from _leaves(arg)
            for _, arg in node[3]:
                yield from _leaves(arg)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _leaves(item)
    elif isinstance(value, slice):
        for item in (value.start, value.stop, value.step):
            yield from _leaves(item)


def leaf(data: Data, column, field: str, length, before: bool, daily: bool) -> Lazy:
    return Lazy(("leaf", Leaf(data, column, field, length, before, daily)))
