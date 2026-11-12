"""A strategy's own computations on price history, done on the host before the run.

A strategy often reads history as an array and computes on it with whatever it likes::

    p = (await data.history(assets=[asset], bar_count=250, fields=["close"]))["close"].to_numpy()
    rets = np.diff(p[-14:]) / p[-14:-1]
    if np.std(rets) * 100 <= config.limit: ...
    rsi = talib.RSI(p, timeperiod=config.period)[-1]

None of it runs on a GPU, and it does not have to: it reads history and numbers, not the
portfolio, so for every bar it is the same in every combination that agrees on the numbers. In a
sweep ``.to_numpy()`` gives a :class:`Lazy` -- a record of what is done to the rows: arithmetic,
indexing and slicing, NumPy's functions (all of them, ``np.maximum.accumulate`` included), other
libraries' functions (TA-Lib, SciPy...), methods. Nothing is computed while the strategy runs.

Where a number is needed -- a comparison, a branch, an order, a ranking -- the record becomes one:
the first time, the sweep notes it (and which distinct values of the config it reads) and starts
the run again; before the run it replays the record with the real rows and the real libraries at
every bar, once per distinct set of those values (:meth:`LazyRegistry.prepare`). At every bar the
step then reads the number like any other history window. The record has to end in a number:
an array the strategy only ever indexes, slices or reduces.

Values that differ between combinations -- a config field, a mask -- enter the record as
parameters: a slice ``p[-config.window:]`` or ``timeperiod=config.period`` is computed for each
distinct value and each combination reads its own. Which row is a combination's is found once a
batch, from what the first bar read; a compiled step only checks it (see
:meth:`LazyRegistry._compiled_value`). A record assigned under a condition that
differs between combinations becomes :class:`Select`: both records, chosen per combination once
they are numbers.
"""
import operator
import os
import pickle
import sys
import types

import numpy as np

from ziplime.sweep.convert import ALL, NotConvertible, is_array
from ziplime.sweep.params import SweepError

_HERE = os.path.dirname(os.path.abspath(__file__))


def where_in_strategy() -> str:
    """``file, line N`` of the strategy's code on the stack: converted code keeps its file and
    lines."""
    frame = sys._getframe(1)
    while frame is not None:
        filename = frame.f_code.co_filename
        # A lambda (a sort key, a comprehension) points at its own first line; the statement that
        # runs it is the one to name
        if not filename.startswith("<") and frame.f_code.co_name != "<lambda>" and \
                not os.path.abspath(filename).startswith(_HERE + os.sep):
            return f"{filename}, line {frame.f_lineno}"
        frame = frame.f_back
    return "the strategy"


# -- the record -----------------------------------------------------------------------------------

class Param:
    """A value that differs between combinations, inside a record."""

    def __init__(self, value):
        self.value = value


class Leaf:
    """``data.history(assets=[asset], bar_count=n, fields=[field])[field]`` at the bar.
    ``column``: the instrument, or ALL inside a loop over the universe -- every one."""

    def __init__(self, data, column, field, length, before, daily):
        self.data, self.column, self.field = data, column, field
        self.length, self.before, self.daily = length, before, daily


class Lazy:
    """A computation on history rows, recorded: ``node`` is (kind, ...)."""

    __array_priority__ = 1000  # NumPy defers to the operators below

    def __init__(self, node):
        object.__setattr__(self, "node", node)

    # -- arithmetic is recorded; comparisons are numbers
    def __add__(self, other):
        return _binary(operator.add, self, other)

    def __radd__(self, other):
        return _binary(operator.add, other, self)

    def __sub__(self, other):
        return _binary(operator.sub, self, other)

    def __rsub__(self, other):
        return _binary(operator.sub, other, self)

    def __mul__(self, other):
        return _binary(operator.mul, self, other)

    def __rmul__(self, other):
        return _binary(operator.mul, other, self)

    def __truediv__(self, other):
        return _binary(operator.truediv, self, other)

    def __rtruediv__(self, other):
        return _binary(operator.truediv, other, self)

    def __floordiv__(self, other):
        return _binary(operator.floordiv, self, other)

    def __rfloordiv__(self, other):
        return _binary(operator.floordiv, other, self)

    def __mod__(self, other):
        return _binary(operator.mod, self, other)

    def __rmod__(self, other):
        return _binary(operator.mod, other, self)

    def __pow__(self, other):
        return _binary(operator.pow, self, other)

    def __rpow__(self, other):
        return _binary(operator.pow, other, self)

    def __neg__(self):
        return record(operator.neg, (self,))

    def __pos__(self):
        return self

    def __abs__(self):
        return record(operator.abs, (self,))

    def __lt__(self, other):
        return _compare(operator.lt, self, other)

    def __le__(self, other):
        return _compare(operator.le, self, other)

    def __gt__(self, other):
        return _compare(operator.gt, self, other)

    def __ge__(self, other):
        return _compare(operator.ge, self, other)

    def __eq__(self, other):
        return _compare(operator.eq, self, other)

    def __ne__(self, other):
        return _compare(operator.ne, self, other)

    __hash__ = object.__hash__

    def __getitem__(self, key):
        return record(operator.getitem, (self, key))

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "ndim",
                                                                         "mask", "_values"):
            raise AttributeError(name)
        return Lazy(("attr", self, name))

    def __call__(self, *args, **kwargs):
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

    def __repr__(self):
        return f"<history computation {self.node[0]}>"


class _Method:
    """``obj.name(...)``, as a function of the object: part of a record's key."""

    def __init__(self, name):
        self.name = name

    def __call__(self, owner, *args, **kwargs):
        return getattr(owner, self.name)(*args, **kwargs)

    def __eq__(self, other):
        return isinstance(other, _Method) and other.name == self.name

    def __hash__(self):
        return hash(("method", self.name))


class Select:
    """A record in some combinations and another value in the rest: ``mask`` chooses."""

    def __init__(self, mask, yes, no):
        self.mask, self.yes, self.no = mask, yes, no

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "ndim"):
            raise AttributeError(name)
        return _distribute(lambda value: getattr(value, name), self)

    def __call__(self, *args, **kwargs):
        return _distribute(lambda value: value(*args, **kwargs), self)

    def __getitem__(self, key):
        return _distribute(lambda value: value[key], self)

    def __bool__(self):
        raise NotConvertible(f"{where_in_strategy()}: an array of history is used as True/False.")

    __hash__ = object.__hash__


def _distribute(function, select: Select):
    return Select(select.mask, function(select.yes), function(select.no))


def _arith(name):
    def method(self, other):
        return _distribute(lambda value: getattr(operator, name)(value, other), self)

    def reflected(self, other):
        return _distribute(lambda value: getattr(operator, name)(other, value), self)
    return method, reflected


for _name in ("add", "sub", "mul", "truediv", "floordiv", "mod", "pow"):
    _forward, _reflected = _arith(_name)
    setattr(Select, f"__{_name}__", _forward)
    setattr(Select, f"__r{_name}__", _reflected)
for _name in ("lt", "le", "gt", "ge", "eq", "ne"):
    setattr(Select, f"__{_name}__",
            (lambda op: lambda self, other: _compare(op, self, other))(getattr(operator, _name)))
Select.__neg__ = lambda self: _distribute(operator.neg, self)
Select.__abs__ = lambda self: _distribute(operator.abs, self)


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


def record(function, args, kwargs=None):
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


def _binary(op, left, right):
    other = right if is_lazy(left) else left
    if is_array(other) or _is_sweep_value(other):
        # A value of the step (the cash, a config field, a signal) meets the record: the record is
        # a number by now, and the arithmetic is the step's
        return op(value_of(left), value_of(right))
    return record(op, (left, right))


def _is_sweep_value(value) -> bool:
    return hasattr(value, "sweep_vectorized") or hasattr(value, "sweep_len")


def _compare(op, left, right):
    return op(value_of(left), value_of(right))


# -- objects initialize put on the context ---------------------------------------------------------

#: Methods that change the object: a model trained during the run is not one a record can replay
CHANGING = {"fit", "partial_fit", "fit_transform", "fit_predict", "set_params", "train", "update",
            "learn", "reset", "reset_states", "compile"}


def is_object_state(value) -> bool:
    """An object of another library initialize keeps on the context -- a fitted model, a scaler:
    not a number, a container, NumPy, pandas, polars or ziplime."""
    if value is None or isinstance(value, (bool, int, float, str, bytes, list, tuple, dict, set,
                                           frozenset, np.ndarray, np.generic)):
        return False
    top = (type(value).__module__ or "").split(".")[0]
    return top not in ("builtins", "ziplime", "numpy", "pandas", "polars", "datetime",
                       "collections", "decimal", "fractions", "pydantic", "types")


class Objects:
    """``context.model``: one object for every combination, or one per distinct run of initialize
    (a model trained with the config's parameters). Its methods on history rows are recorded --
    ``context.model.predict(features)`` is then replayed before the run like ``talib.RSI(p)``, and
    which object a combination uses is a parameter of the record, so each model is asked once.
    ``index``: None for one object, or (combinations × 1) positions in ``objects``."""

    def __init__(self, objects: list, index=None):
        object.__setattr__(self, "_objects", objects)
        object.__setattr__(self, "_index", index)

    def __getattr__(self, name):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype", "ndim"):
            raise AttributeError(name)
        if name in CHANGING:
            raise NotConvertible(f"{where_in_strategy()}: .{name}() changes an object kept on the "
                                 f"context while the run goes on; a sweep replays what is computed "
                                 f"from history before the run. Train it in initialize (it may "
                                 f"read the config) or in compute_signals.")
        attribute = getattr(self._objects[0], name)
        if not callable(attribute):
            if self._index is None:
                return attribute
            raise NotConvertible(f"{where_in_strategy()}: .{name} of an object that differs "
                                 f"between combinations is not supported in a sweep.")
        return _ObjectMethod(self, name)

    def __setattr__(self, name, value):
        raise NotConvertible(f"{where_in_strategy()}: an object kept on the context is changed "
                             f"(.{name} = ...) during the run; a sweep cannot follow that.")


class _ObjectCall:
    """``objects[position].name(*args)``: the function a record of a method call keeps."""

    def __init__(self, objects: list, name: str):
        self.objects, self.name = objects, name

    def __call__(self, position, *args, **kwargs):
        return getattr(self.objects[int(position)], self.name)(*args, **kwargs)

    def _identity(self):
        # The objects themselves come from initialize's results and stay; the list holding them
        # is made anew with every run of the batch
        return tuple(map(id, self.objects)), self.name

    def __eq__(self, other):
        return isinstance(other, _ObjectCall) and other._identity() == self._identity()

    def __hash__(self):
        return hash(self._identity())

    @property
    def __name__(self):
        return f"{type(self.objects[0]).__name__}.{self.name}"


class _ObjectMethod:
    def __init__(self, owner: Objects, name: str):
        self.owner, self.name = owner, name

    def __call__(self, *args, **kwargs):
        owner = self.owner
        if contains_lazy(args, kwargs):
            position = 0 if owner._index is None else owner._index
            return record(_ObjectCall(owner._objects, self.name), (position, *args), kwargs)
        if any(is_array(a) or a is ALL or _is_sweep_value(a) for a in (*args, *kwargs.values())):
            raise NotConvertible(f"{where_in_strategy()}: .{self.name}(...) of an object kept on "
                                 f"the context gets values that differ between combinations. On "
                                 f"features computed from history rows (data.history(...)"
                                 f"['close'].to_numpy()) it is computed ahead of the run.")
        if owner._index is not None:
            raise NotConvertible(f"{where_in_strategy()}: .{self.name}(...) of an object that "
                                 f"differs between combinations, on plain numbers.")
        return getattr(owner._objects[0], self.name)(*args, **kwargs)


# -- turning a record into a number ---------------------------------------------------------------

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


def leaf(data, column, field, length, before, daily) -> Lazy:
    return Lazy(("leaf", Leaf(data, column, field, length, before, daily)))


class _Template:
    """A record's key and what replaying it needs: its parameters in order, and whether it
    reads the loop's instrument (then it is computed for every instrument)."""

    def __init__(self, value: Lazy):
        self.params = []
        self.per_column = False
        self.key = self._key(value)

    def _key(self, value):
        if isinstance(value, Lazy):
            node = value.node
            if node[0] == "leaf":
                spec = node[1]
                if spec.column is ALL:
                    self.per_column = True
                return ("leaf", "all" if spec.column is ALL else spec.column, spec.field,
                        self._key(_wrap(spec.length)), spec.before, spec.daily)
            if node[0] == "attr":
                return ("attr", self._key(node[1]), node[2])
            return ("call", _function_key(node[1]), tuple(self._key(arg) for arg in node[2]),
                    tuple((name, self._key(arg)) for name, arg in node[3]))
        if isinstance(value, Param):
            self.params.append(value.value)
            return ("param", len(self.params) - 1)
        if isinstance(value, slice):
            return ("slice", self._key(value.start), self._key(value.stop), self._key(value.step))
        if isinstance(value, (list, tuple)):
            return (type(value).__name__, tuple(self._key(item) for item in value))
        if isinstance(value, float) and value != value:
            return ("nan",)
        if isinstance(value, (dict, set, frozenset)):
            return ("object", id(value))
        if callable(value):
            return ("function", _function_key(value))
        return ("value", type(value).__name__, value)


def _function_key(function):
    try:
        hash(function)
        return function
    except TypeError:
        return ("object", id(function))


def replay(value, rows, t: int, column: int, params: tuple):
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


class _Rows:
    """History rows at a bar, as the engine's polars Series once anything but to_numpy is
    asked of them."""

    def __init__(self, values: np.ndarray, field: str):
        self.values, self.field = values, field

    def series(self):
        import polars as pl
        return pl.Series(self.field, self.values)


def _real(value):
    return value.series() if isinstance(value, _Rows) else value


def number(result) -> float:
    if result is None:
        return np.nan
    if np.ndim(result) != 0:
        raise TypeError(f"an array of shape {np.shape(result)}")
    return float(result)


# -- replaying a record fast -----------------------------------------------------------------------

class _Unrunnable(Exception):
    """A record that holds an object only this process has (a dict, a lambda): replayed walking
    the record itself."""


def compile_plan(key, rows):
    """A record's key -- which describes the record completely -- as a function
    ``(t, column, params) -> value``: what :func:`replay` computes, without walking the record at
    every bar. ``rows``: HistoryRows."""
    tag = key[0]
    if tag == "leaf":
        _, where, field, length, before, daily = key
        length_of = compile_plan(length, rows)

        def leaf_rows(t, c, P):
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


def replay_series(plan, n_bars: int, columns, params) -> np.ndarray:
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


# -- TA-Lib functions computed once on the whole history ------------------------------------------

#: TA-Lib functions of one input and one output whose last value reads only the input's last
#: ``lookback + 1`` points: on a window of history it is the value at the window's last row of the
#: function run once over the whole history. The smoothed ones (EMA, RSI, MACD, ATR...) remember
#: where their input began and are not here. tests/sweep/test_sweep_lazy.py checks every one.
WINDOW_LOCAL = {"ROC", "ROCP", "ROCR", "ROCR100", "MOM", "SMA", "WMA", "TRIMA", "SUM", "MAX", "MIN",
                "MIDPOINT", "STDDEV", "VAR", "LINEARREG", "LINEARREG_SLOPE",
                "LINEARREG_INTERCEPT", "LINEARREG_ANGLE", "TSF",
                # several inputs of one instrument's window (high, low, close...)
                "WILLR", "CCI", "AROONOSC", "MIDPRICE", "TRANGE", "MEDPRICE", "TYPPRICE",
                "WCLPRICE", "AVGPRICE", "CORREL", "BETA", "MFI", "ULTOSC",
                # several outputs: [j][-1]
                "AROON", "MINMAX", "BBANDS", "STOCHF", "STOCH"}


class _NotFast(Exception):
    pass


def _constant(key, params):
    if key[0] == "value":
        return key[2]
    if key[0] == "param":
        return params[key[1]]
    raise _NotFast()


def _window_of(key, params):
    """``rows.to_numpy()`` or ``rows.to_numpy()[-k:]``: (leaf key, k or None)."""
    k = None
    if key[0] == "call" and key[1] is operator.getitem and len(key[2]) == 2 \
            and key[2][1][0] == "slice":
        start, stop, step = key[2][1][1:]
        start = _constant(start, params)
        if _constant(stop, params) is not None or _constant(step, params) is not None or \
                not isinstance(start, int) or start >= 0:
            raise _NotFast()
        key, k = key[2][0], -start
    if not (key[0] == "call" and isinstance(key[1], _Method) and key[1].name == "to_numpy"
            and len(key[2]) == 1 and not key[3] and key[2][0][0] == "leaf"):
        raise _NotFast()
    leaf = key[2][0]
    if leaf[5]:  # days built from intraday rows: a window of its own, not a slice of one series
        raise _NotFast()
    return leaf, k


#: Arithmetic on numbers computed from history: elementwise over the bars it gives what it gives
#: at each bar -- the values are NumPy float64 at a bar too, with the same NaN and division rules.
ELEMENTWISE = {operator.add, operator.sub, operator.mul, operator.truediv, operator.pow,
               operator.neg, operator.abs}


def fast_series(key, params, market, columns) -> np.ndarray | None:
    """A record at every bar without replaying it bar by bar, if it is made of
    ``talib.X(history[-k:], ...)[-1]`` for functions in WINDOW_LOCAL -- each run once over each
    instrument's whole history and read at the bar's last row, the same number as the window's --
    numbers and arithmetic. None for any other record, or for history with gaps of NaN (the
    function would carry them along the series, not within one window)."""
    try:
        with np.errstate(all="ignore"):
            result = _series(key, params, market, columns)
    except (_NotFast, IndexError, TypeError):
        return None
    if not isinstance(result, np.ndarray):
        return None
    return np.broadcast_to(result, (market.n_bars, len(columns))).astype(np.float64)


def _series(key, params, market, columns):
    if key[0] in ("value", "param"):
        value = _constant(key, params)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _NotFast()
        return value
    if key[0] != "call" or key[3]:
        raise _NotFast()
    if key[1] is operator.getitem and len(key[2]) == 2 and key[2][1][0] in ("value", "param"):
        try:
            return _row_of_window(key, params, market, columns)
        except _NotFast:
            return _last_of_window_local(key, params, market, columns)
    if key[1] in ELEMENTWISE:
        return key[1](*[_series(arg, params, market, columns) for arg in key[2]])
    raise _NotFast()


def _row_of_window(key, params, market, columns) -> np.ndarray:
    """``history[-k:][j]``: one row of the window -- the window's first (``[0]``) or last
    (``[-1]``) price -- at every bar, by index; NaN where the window has no such row (an
    IndexError at that bar)."""
    position = _constant(key[2][1], params)
    if isinstance(position, bool) or not isinstance(position, int):
        raise _NotFast()
    leaf, k = _window_of(key[2][0], params)
    _, where, field, length, before, _ = leaf
    length = int(_constant(length, params))
    ends_all = market.history_end_before if before else market.history_end
    result = np.full((market.n_bars, len(columns)), np.nan)
    for j, column in enumerate(columns):
        column = column if where == "all" else where
        ends = ends_all[:, column].astype(np.int64)
        width = np.minimum(ends, length if k is None else min(length, k))
        row = ends - width + position if position >= 0 else ends + position
        valid = (position < width) if position >= 0 else (-position <= width)
        values = market.rows[field][:, column]
        result[:, j] = np.where(valid, values[np.clip(row, 0, len(values) - 1)], np.nan)
    return result


def _last_of_window_local(key, params, market, columns) -> np.ndarray:
    """``talib.X(window, ...)[-1]``, or ``talib.X(window...)[j][-1]`` for its j-th output, with
    one or more windows of one instrument's history (high, low, close)."""
    if _constant(key[2][1], params) != -1:
        raise _NotFast()
    inner, output = key[2][0], None
    if inner[0] == "call" and inner[1] is operator.getitem and len(inner[2]) == 2 \
            and inner[2][1][0] in ("value", "param"):
        output = _constant(inner[2][1], params)
        if isinstance(output, bool) or not isinstance(output, int):
            raise _NotFast()
        inner = inner[2][0]
    if inner[0] != "call" or isinstance(inner[1], (tuple, _Method)) or not inner[2]:
        raise _NotFast()
    function = inner[1]
    if not (getattr(function, "__module__", "") or "").startswith("talib") or \
            getattr(function, "__name__", None) not in WINDOW_LOCAL:
        raise _NotFast()
    windows = []
    for arg in inner[2]:
        try:
            windows.append(_window_of(arg, params))
        except _NotFast:
            break
    if not windows:
        raise _NotFast()
    shape = {(leaf[1], _constant(leaf[3], params), leaf[4], k) for leaf, k in windows}
    if len(shape) != 1:
        raise _NotFast()  # windows of different instruments or lengths
    fields = [leaf[2] for leaf, _ in windows]
    (leaf, k) = windows[0]
    rest = [_constant(arg, params) for arg in inner[2][len(windows):]]
    options = {name: _constant(arg, params) for name, arg in inner[3]}
    _, where, _, length, before, _ = leaf
    length = int(_constant(length, params))
    import talib.abstract

    described = talib.abstract.Function(function.__name__)
    names = list(described.parameters)
    described.set_parameters(**dict(zip(names, rest)), **options)
    if any(name.endswith("matype") and value != 0 for name, value in described.parameters.items()):
        raise _NotFast()  # an exponential or other smoothed average remembers where it began
    lookback = described.lookback
    ends_all = market.history_end_before if before else market.history_end
    result = np.full((market.n_bars, len(columns)), np.nan)
    for j, column in enumerate(columns):
        column = column if where == "all" else where
        ends = ends_all[:, column].astype(np.int64)
        count = int(ends.max()) if len(ends) else 0
        if count == 0:
            continue
        inputs = []
        for field in fields:
            if field not in market.rows:
                raise _NotFast()
            series = np.ascontiguousarray(market.rows[field][:count, column], dtype=np.float64)
            if np.isnan(series).any():
                raise _NotFast()
            inputs.append(series)
        try:
            out = function(*inputs, *rest, **options)
        except Exception:
            raise _NotFast() from None
        if isinstance(out, tuple) != (output is not None):
            raise _NotFast()
        if output is not None:
            out = out[output]
        width = np.minimum(ends, length if k is None else min(length, k))
        valid = (ends > 0) & (width > lookback)
        result[:, j] = np.where(valid, out[np.maximum(ends - 1, 0)], np.nan)
    return result


# -- replaying on every core ----------------------------------------------------------------------

#: Replays past this many (bar, record) evaluations go to the CPU's cores. One costs about 3 µs
#: here; starting the processes about 2.5 s (each imports NumPy, TA-Lib and this module). On an M3
#: 3 million evaluations took 8.0 s here and 4.6 s on 8 cores, 0.7 million 1.8 s and 3.4 s.
PARALLEL_FROM = 2_000_000

_WORKER = {}


def _start_worker(market_arrays: dict):
    from ziplime.sweep.market import HistoryRows

    market = types.SimpleNamespace(**market_arrays)
    _WORKER["rows"], _WORKER["n_bars"] = HistoryRows(market), market.n_bars
    os.environ["ZIPLIME_SWEEP_REPLAY_WORKERS"] = "1"


def _replay_job(job):
    key, params, columns = job
    plan = compile_plan(key, _WORKER["rows"])
    return replay_series(plan, _WORKER["n_bars"], columns, params)


def replay_workers() -> int:
    return max(1, int(os.environ.get("ZIPLIME_SWEEP_REPLAY_WORKERS", os.cpu_count() or 1)))


# -- where the numbers come from: the registry ----------------------------------------------------

class LazyRegistry:
    """Every record the strategy turned into a number, and its values at every bar.

    Lives on the run's Windows: ``now`` and ``stacks`` are the windows' own, so the step's inputs
    carry these values as they carry history windows (compiled steps included)."""

    def __init__(self, windows):
        self.windows = windows
        #: key -> stack name, record (for replaying), per_column
        self.records = {}
        #: stack name -> sorted parameter values computed
        self.values = {}
        self._cache = {}
        self._probes = {}
        #: Each record's parameter values per combination, as discovery saw them, by the record
        #: and the order it was asked for within the bar: (name, k) -> [tuple per combination]
        self._seen = {}
        self._occurrence = {}
        #: (name, k) -> each combination's row of the record's values, worked out once a batch
        self._rows = {}
        #: Whether the step is being compiled, and the combinations whose parameter was not the
        #: one their row was looked up for (see value())
        self.compiled = False
        self.misses = []
        #: How the records' values were computed: once on the whole history, replayed at every
        #: bar here, or on the CPU's cores
        self.stats = {"whole history": 0, "replayed": 0, "replayed on cores": 0}
        #: key -> how its values were computed; record name -> where the strategy uses it
        self.method, self.where = {}, {}

    def _name(self, template: _Template, value: Lazy) -> str:
        entry = self.records.get(template.key)
        if entry is None:
            entry = self.records[template.key] = (f"lazy{len(self.records)}", value,
                                                  template.per_column)
            #: where the strategy first needed the record as a number, for the report
            self.where[entry[0]] = where_in_strategy()
        return entry[0]

    def how(self, key) -> str:
        """How a record's values were computed, for the report."""
        return self.method.get(key, "not computed")

    def always_nan(self, key) -> bool:
        """Whether a record is NaN at every bar for every set of its parameters."""
        name = self.records[key][0]
        series = [self._cache.get((key, params)) for params in self.values.get(name, [()])]
        series = [values for values in series if values is not None]
        return bool(series) and all(np.isnan(values).all() for values in series)

    def _host(self, params):
        """The parameters' values per combination, on the host; None while a compiled step
        traces them."""
        try:
            return [np.asarray(self.windows.backend.numpy(p), dtype=float).reshape(-1)
                    for p in params]
        except Exception:
            return None

    def begin_step(self):
        """A bar starts: records are asked for in the same order at every bar."""
        self._occurrence = {}
        self.misses = []

    def begin_discovery(self):
        """A batch starts: its combinations' parameter values are seen anew."""
        self._seen, self._rows = {}, {}

    def _rows_of(self, params) -> list | None:
        """Each combination's parameter values, as tuples; None while a compiled step traces."""
        host = self._host(params)
        if host is None:
            return None
        length = max(len(h) for h in host)
        host = [np.broadcast_to(h, (length,)) for h in host]
        # Whole numbers stay ints: they are bar counts and slice bounds
        return [tuple(int(v) if float(v).is_integer() else v for v in row)
                for row in zip(*[h.tolist() for h in host])]

    def _request(self, name, params, k=None) -> bool:
        """Note the parameter values seen; True if any were new."""
        known = set(self.values.get(name, ()))
        if not params:
            new = {()}
        else:
            rows = self._rows_of(params)
            if rows is None:
                return False
            if k is not None:
                self._seen[(name, k)] = rows
            new = set(rows)
        if new <= known and name in self.values:
            return False
        self.values[name] = sorted(known | new)
        return True

    def _row_index(self, name, k, combinations: int):
        """Each combination's row of the record's values for its k-th use in the bar, from the
        values discovery saw -- once a batch, not at every bar; None if discovery did not see it."""
        if (name, k) not in self._rows:
            seen = self._seen.get((name, k))
            position = {row: n for n, row in enumerate(self.values.get(name, ()))}
            if seen is None or len(seen) not in (1, combinations) or \
                    any(row not in position for row in seen):
                self._rows[(name, k)] = None
            else:
                self._rows[(name, k)] = np.array([position[row] for row in seen], dtype=np.int32)
        return self._rows[(name, k)]

    def value(self, value: Lazy):
        from ziplime.sweep.engine import Rediscover

        windows, xp = self.windows, self.windows.backend.xp
        template = _Template(value)
        name = self._name(template, value)
        k = self._occurrence[name] = self._occurrence.get(name, -1) + 1
        width = len(windows.market.columns) if template.per_column else 1
        if windows.discovering:
            self._request(name, template.params, k)
            return xp.zeros((1, width)) + np.nan
        if not self.compiled and self._request(name, template.params) or name not in windows.now:
            raise Rediscover()
        rows = windows.now[name]  # (distinct parameter values, width)
        if not template.params:
            return rows
        table = self.values[name]
        if self.compiled:
            return self._compiled_value(name, k, rows, table, template.params)
        return xp.take(rows, self._match(table, template.params), axis=0)

    def _compiled_value(self, name, k, rows, table, params):
        """In a compiled step the parameters cannot be read on the host. Each combination's row
        was found once for the batch from what discovery saw; the step only checks, one
        comparison a combination, that the parameter is still the row's value. Where it is not
        -- a parameter that changed during the run -- the combination is noted, and the batch is
        run again without compiling, where every bar checks on the host."""
        xp, backend = self.windows.backend.xp, self.windows.backend
        combinations = max(int(param.shape[0]) for param in params)
        found = self._row_index(name, k, combinations)
        index = backend.xp.array(found) if found is not None else self._match(table, params)
        same = None
        for p, param in enumerate(params):
            column = backend.array(np.array([row[p] for row in table], dtype=float))
            hit = xp.take(column, index, axis=0)[:, None] == param
            same = hit if same is None else xp.logical_and(same, hit)
        self.misses.append(xp.logical_not(same))
        return xp.take(rows, index, axis=0)

    def _match(self, table, params):
        """Each combination's row of the record's values, looked up on the device."""
        xp, backend = self.windows.backend.xp, self.windows.backend
        index = None
        for p, param in enumerate(params):
            column = backend.array(np.array([row[p] for row in table], dtype=float))
            hit = param == column[None, :]
            index = hit if index is None else xp.logical_and(index, hit)
        return xp.argmax(index.astype(xp.int32), axis=1).astype(xp.int32)

    def is_number(self, value) -> bool:
        """Whether the record ends in a number (or an array): replayed once at the last bar."""
        if isinstance(value, Select):
            side = value.yes if is_lazy(value.yes) else value.no
            return self.is_number(side)
        template = _Template(value)
        if template.key not in self._probes:
            from ziplime.sweep.market import HistoryRows

            market = self.windows.market
            params = tuple(self._first(p) for p in template.params)
            try:
                result = replay(value, HistoryRows(market), market.n_bars - 1, 0, params)
                self._probes[template.key] = np.ndim(result) == 0
            except Exception:
                self._probes[template.key] = True
        return self._probes[template.key]

    def _first(self, param):
        v = float(np.asarray(self.windows.backend.numpy(param)).reshape(-1)[0])
        return int(v) if v.is_integer() else v

    def prepare(self):
        """Every record's value at every bar, for each distinct set of parameter values: a TA-Lib
        function of one window once on the whole history (fast_series), anything else replayed
        bar by bar -- on the CPU's cores when there is enough of it."""
        from ziplime.sweep.market import HistoryRows

        market = self.windows.market
        rows = HistoryRows(market)
        pending = []
        for key, (name, value, per_column) in self.records.items():
            columns = list(range(len(market.columns))) if per_column else [0]
            for params in self.values.get(name, [()]):
                if (key, params) in self._cache:
                    continue
                fast = fast_series(key, params, market, columns)
                if fast is not None:
                    self._cache[(key, params)] = fast
                    self.stats["whole history"] += 1
                    self.method[key] = "once on the whole history"
                else:
                    pending.append((key, params, columns, value))
        self._replay_pending(pending, rows, market)
        for key, (name, value, per_column) in self.records.items():
            table = self.values.get(name, [()])
            width = len(market.columns) if per_column else 1
            stack = np.full((market.n_bars, len(table), width), np.nan)
            for k, params in enumerate(table):
                stack[:, k, :] = self._cache[(key, params)]
            self.windows.stacks[name] = self.windows.backend.array(stack)

    def _replay_pending(self, pending, rows, market):
        work = sum(market.n_bars * len(columns) for _, _, columns, _ in pending)
        jobs, local = [], []
        for key, params, columns, value in pending:
            try:
                compile_plan(key, rows)
                pickle.dumps(key)
                jobs.append((key, params, columns))
            except (_Unrunnable, pickle.PicklingError, TypeError, AttributeError):
                local.append((key, params, columns, value))
        workers = min(replay_workers(), len(jobs))
        if workers > 1 and work >= PARALLEL_FROM:
            import concurrent.futures
            import multiprocessing

            arrays = dict(rows=market.rows, history_end=market.history_end,
                          history_end_before=market.history_end_before, row_days=market.row_days,
                          n_bars=market.n_bars)
            with concurrent.futures.ProcessPoolExecutor(
                    max_workers=workers, mp_context=multiprocessing.get_context("spawn"),
                    initializer=_start_worker, initargs=(arrays,)) as pool:
                chunk = max(1, len(jobs) // (workers * 4))
                for (key, params, _), series in zip(jobs, pool.map(_replay_job, jobs,
                                                                    chunksize=chunk)):
                    self._cache[(key, params)] = series
                    self.method[key] = f"bar by bar, on {workers} of the CPU's cores"
            self.stats["replayed on cores"] += len(jobs)
        else:
            for key, params, columns in jobs:
                plan = compile_plan(key, rows)
                self._cache[(key, params)] = replay_series(plan, market.n_bars, columns, params)
                self.method[key] = "bar by bar"
            self.stats["replayed"] += len(jobs)
        for key, params, columns, value in local:
            series = np.full((market.n_bars, len(columns)), np.nan)
            for j, column in enumerate(columns):
                for t in range(market.n_bars):
                    series[t, j] = self._replay(value, rows, t, column, params)
            self._cache[(key, params)] = series
            self.stats["replayed"] += 1
            self.method[key] = "bar by bar"

    @staticmethod
    def _replay(value, rows, t, column, params):
        try:
            result = replay(value, rows, t, column, params)
        except NotConvertible:
            raise
        except Exception:
            # The engine would raise at this bar -- but only where the strategy takes this
            # branch, which a sweep, running every branch, cannot know here
            return np.nan
        try:
            return number(result)
        except TypeError as error:
            raise SweepError(f"A computation on history is used as a number but gives {error}: "
                             f"index it ([-1]) or reduce it (np.mean, .max()) first.") from None
