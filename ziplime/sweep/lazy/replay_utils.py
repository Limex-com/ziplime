"""Replay utils for lazy."""

from collections.abc import Callable
import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.params import SweepError
from ziplime.sweep.market import HistoryRows
from ziplime.sweep.lazy.lazy import Lazy
from ziplime.sweep.lazy.values import Param
from ziplime.sweep.lazy.method import _Method
from ziplime.sweep.lazy.rows import _Rows
from ziplime.sweep.lazy.unrunnable import _Unrunnable
from ziplime.sweep.lazy.recording import _wrap


def _function_key(function: Callable):
    try:
        hash(function)
        return function
    except TypeError:
        return ("object", id(function))


def replay(value, rows: HistoryRows, t: int, column: int, params: tuple):
    """The record with real rows at bar ``t`` for instrument ``column`` and these parameter
    values. ``rows``: HistoryRows."""
    position = [0]

    def run(item):
        if isinstance(item, Lazy):
            node = item.node
            if node[0] == "leaf":
                spec = node[1]
                return _Rows(rows.window(spec.field, column if spec.column is ALL else spec.column,
                                         t, run(_wrap(spec.length)), spec.before, spec.daily),
                             spec.field)
            if node[0] == "attr":
                return getattr(_real(run(node[1])), node[2])
            function = node[1]
            args = [run(arg) for arg in node[2]]
            kwargs = {name: run(arg) for name, arg in node[3]}
            if isinstance(function, _Method):
                owner = args[0]
                if isinstance(owner, _Rows) and function.name in ("to_numpy", "to_list"):
                    return owner.values.copy() if function.name == "to_numpy" else \
                        owner.values.tolist()
                return getattr(_real(owner), function.name)(*map(_real, args[1:]),
                                                            **{k: _real(v) for k, v in kwargs.items()})
            return function(*map(_real, args), **{k: _real(v) for k, v in kwargs.items()})
        if isinstance(item, Param):
            k = position[0]
            position[0] += 1
            return params[k]
        if isinstance(item, slice):
            return slice(run(item.start), run(item.stop), run(item.step))
        if isinstance(item, (list, tuple)):
            return type(item)(run(each) for each in item)
        return item

    return _real(run(value))


def _real(value):
    return value.series() if isinstance(value, _Rows) else value


def number(result) -> float:
    if result is None:
        return np.nan
    if np.ndim(result) != 0:
        raise TypeError(f"an array of shape {np.shape(result)}")
    return float(result)


def compile_plan(key, rows: HistoryRows):
    """A record's key -- which describes the record completely -- as a function
    ``(t, column, params) -> value``: what :func:`replay` computes, without walking the record at
    every bar. ``rows``: HistoryRows."""
    tag = key[0]
    if tag == "leaf":
        _, where, field, length, before, daily = key
        length_of = compile_plan(length, rows)

        def leaf_rows(t, c, P) -> _Rows:
            column = c if where == "all" else where
            return _Rows(rows.window(field, column, t, length_of(t, c, P), before, daily), field)
        return leaf_rows
    if tag == "param":
        i = key[1]
        return lambda t, c, P: P[i]
    if tag == "value":
        value = key[2]
        return lambda t, c, P: value
    if tag == "nan":
        return lambda t, c, P: np.nan
    if tag == "function":
        if isinstance(key[1], tuple):
            raise _Unrunnable()
        function = key[1]
        return lambda t, c, P: function
    if tag == "slice":
        parts = [compile_plan(part, rows) for part in key[1:]]
        return lambda t, c, P: slice(*(part(t, c, P) for part in parts))
    if tag in ("list", "tuple"):
        kind = list if tag == "list" else tuple
        items = [compile_plan(item, rows) for item in key[1]]
        return lambda t, c, P: kind(item(t, c, P) for item in items)
    if tag == "attr":
        owner, name = compile_plan(key[1], rows), key[2]
        return lambda t, c, P: getattr(_real(owner(t, c, P)), name)
    if tag == "call":
        function = key[1]
        if isinstance(function, tuple):
            raise _Unrunnable()
        args = [compile_plan(arg, rows) for arg in key[2]]
        kwargs = [(name, compile_plan(arg, rows)) for name, arg in key[3]]
        if isinstance(function, _Method):
            name = function.name

            def method(t, c, P):
                owner = args[0](t, c, P)
                if isinstance(owner, _Rows) and name in ("to_numpy", "to_list"):
                    return owner.values.copy() if name == "to_numpy" else owner.values.tolist()
                return getattr(_real(owner), name)(*[_real(a(t, c, P)) for a in args[1:]],
                                                   **{n: _real(a(t, c, P)) for n, a in kwargs})
            return method

        def call(t, c, P):
            return function(*[_real(a(t, c, P)) for a in args],
                            **{n: _real(a(t, c, P)) for n, a in kwargs})
        return call
    raise _Unrunnable()


def replay_series(plan: Callable, n_bars: int, columns: list[int], params) -> np.ndarray:
    """A compiled record's value at every bar, for each of ``columns``: (bars, columns)."""
    result = np.full((n_bars, len(columns)), np.nan)
    for j, column in enumerate(columns):
        for t in range(n_bars):
            try:
                value = _real(plan(t, column, params))
            except NotConvertible:
                raise
            except Exception:
                continue  # NaN, as LazyRegistry._replay explains
            try:
                result[t, j] = number(value)
            except TypeError as error:
                raise SweepError(f"A computation on history is used as a number but gives "
                                 f"{error}: index it ([-1]) or reduce it (np.mean, .max()) "
                                 f"first.") from None
    return result
