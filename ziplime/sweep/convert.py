"""An ordinary ``handle_data``, run for every combination at once.

On a GPU every combination executes the same instructions, so Python that branches on a value --
``if fast > slow`` -- cannot run as written: the answer differs between combinations. :func:`convert`
reads the function's source and rewrites it mechanically, as TensorFlow's AutoGraph does:

    in the strategy                              after conversion
    -----------------------------------------    ------------------------------------------------
    for name, asset in context.universe...       one pass; name and asset stand for every instrument
    context.signals["fast"][name]                an array (combinations × instruments)
    if a > b: A  else: B                         both A and B run, each under its own mask
    x = ...  inside an if                        x changes only where the mask holds
    continue / return / return value             the mask switches the rest off for those lanes
    a and b, not a, x if c else y                elementwise and, not, where
    await context.order_target_percent(...)      an order only where the mask holds
    helper(x), a function of the same module     converted the same way

A value that is the same for every combination (a counter, ``len(context.universe)``) stays a
plain Python value and an ``if`` on it is an ordinary ``if``. What cannot be converted -- a loop over
instruments where one instrument's decision depends on the previous one, ``break``/``while`` on a
value that differs between combinations, ``try`` -- is refused with the line and the reason.
"""
import ast
import builtins
import copy
import inspect
import math
import os
import sys
import textwrap
import types

import numpy as np


class NotConvertible(Exception):
    """The function cannot run for every combination at once."""


class _AllAssets:
    """The name and the instrument inside a loop over context.universe: every instrument.

    ``name == "JNJ"`` or ``asset == context.universe["JNJ"]`` is a mask over the instruments."""

    #: Set by the simulation: (names, listings, make an array)
    _columns = None

    def __repr__(self):
        return "<every instrument>"

    def __eq__(self, other):
        if other is self:
            return True
        if hasattr(other, "sweep_equals_all"):
            return other.sweep_equals_all()
        names, listings, array = self.__dict__.get("_columns") or type(self)._columns
        return array(np.array([[name == other or listing == other
                                or getattr(listing, "sid", None) == other
                                for name, listing in zip(names, listings)]]))

    def __ne__(self, other):
        equal = self.__eq__(other)
        return (not equal) if isinstance(equal, bool) else ~equal

    __hash__ = object.__hash__

    @property
    def sid(self):
        """``asset.sid``: every instrument's sid -- a key and a filter like the instrument."""
        return self

    def __getattr__(self, name):
        raise NotConvertible(
            f"Inside a loop over context.universe one pass handles every instrument at once, so "
            f"the instrument has no single .{name}.")


ALL = _AllAssets()


class Vectorized(list):
    """A loop over context.universe: one pass, for every instrument. ``mask``: the instruments the
    pass is for -- a per-instrument dict's keys, say -- True for all of them."""

    mask = True

    def __init__(self, items, mask=True):
        super().__init__(items)
        self.mask = mask


class VectorUniverse(dict):
    """context.universe in a sweep: len and lookup by name as a dict, iteration in one pass."""

    def items(self):
        return Vectorized([(ALL, ALL)])

    def keys(self):
        return Vectorized([ALL])

    def values(self):
        return Vectorized([ALL])

    def __iter__(self):
        return iter(Vectorized([ALL]))


def is_array(value) -> bool:
    return hasattr(value, "shape") and not isinstance(value, np.generic)


# -- what converted code calls ------------------------------------------------------------------

class Runtime:
    """Masks and elementwise logic. A mask is True/False (the same everywhere) or an array."""

    def __init__(self, xp):
        self.xp = xp
        #: The current statement's mask: no order and no assignment where it is False
        self.mask = True
        #: NaN and infinity as arrays the simulation hands in. MLX writes a NaN or infinity
        #: constant into a fused Metal kernel as an undeclared identifier and the compile fails,
        #: so inside a compiled step they must be inputs, never literals.
        self.nan, self.inf = np.nan, np.inf

    def truth(self, value):
        from ziplime.sweep.lazy import is_lazy, value_of
        if is_lazy(value):
            value = value_of(value)
        elif hasattr(value, "sweep_truth"):
            value = value.sweep_truth()
        elif not is_array(value) and hasattr(value, "sweep_len"):
            count = value.sweep_len()
            value = count > 0 if is_array(count) else bool(count)
        if is_array(value):
            return value if value.dtype in (np.bool_, getattr(self.xp, "bool_", np.bool_)) \
                else value != 0
        return bool(value)

    def and_(self, left, right):
        """``left and right``; right is a function, not evaluated when left is False."""
        left = self.truth(left)
        if left is False:
            return False
        right = self.truth(right())
        if left is True:
            return right
        if right is True:
            return left
        if right is False:
            return False
        return self.xp.logical_and(left, right)

    def or_(self, left, right):
        left = self.truth(left)
        if left is True:
            return True
        right = self.truth(right())
        if left is False:
            return right
        if right is False:
            return left
        if right is True:
            return True
        return self.xp.logical_or(left, right)

    def not_(self, value):
        value = self.truth(value)
        return (not value) if isinstance(value, bool) else self.xp.logical_not(value)

    def alive(self, dead, skip):
        return self.not_(self.or_(dead, lambda: skip))

    def sel(self, mask, new, old):
        """Assignment under a mask: the new value where it holds, the previous one elsewhere."""
        if mask is True:
            return new
        try:
            previous = old()
        except (NameError, AttributeError, KeyError, IndexError):
            return new  # not defined before -- where the mask is False it is never read
        if mask is False:
            return previous
        return self.choose(mask, new, previous)

    def choose(self, mask, new, previous):
        """``new`` where the mask holds, ``previous`` elsewhere: for numbers, computations on
        history (chosen once they are numbers) and lists of instruments."""
        from ziplime.sweep import cross
        from ziplime.sweep.lazy import Select, is_lazy

        if is_lazy(new) or is_lazy(previous):
            return Select(mask, new, previous)
        merged = cross.merge_instruments(self.xp, mask, new, previous)
        if merged is not None:
            return merged
        if previous is None or new is None:
            raise NotConvertible("A variable is None in some combinations and a number in others; "
                                 "give it a number (0, float('nan')) instead of None.")
        return self.xp.where(mask, new, previous)

    def returned(self, mask, value, previous):
        """A ``return value`` under a mask: lanes that returned earlier keep their value."""
        if mask is True or previous is None:
            return value
        if mask is False:
            return previous
        return self.xp.where(mask, value, previous)

    def where(self, condition, yes, no):
        condition = self.truth(condition)
        if isinstance(condition, bool):
            return yes if condition else no
        return self.choose(condition, yes, no)

    def sort_list(self, items, key=None, reverse=False):
        """``items.sort(key=..., reverse=...)``: a list of instruments ranked per combination;
        any other list sorted in place, as Python does."""
        from ziplime.sweep import cross
        ranked = cross.sort_instruments(self, items, key, reverse)
        if ranked is not None:
            return ranked
        items.sort(key=key, reverse=reverse)
        return items

    def is_none(self, value):
        """``value is None``: a position that is not open is None in the engine."""
        if hasattr(value, "sweep_truth"):
            return self.not_(value.sweep_truth())
        return value is None

    def nested(self, function):
        """A function defined inside a converted one: it runs under the mask it is called with."""
        def call(*args, **kwargs):
            saved = self.mask
            try:
                return function(*args, **kwargs)
            finally:
                self.mask = saved
        return call

    def plain(self, condition, line, what):
        """A condition Python has to decide by itself: it must be the same everywhere."""
        condition = self.truth(condition)
        if isinstance(condition, bool):
            return condition
        raise NotConvertible(f"line {line}: {what} on a value that differs between combinations "
                             f"or instruments cannot run on a GPU.")

    def loop_mask(self, iterable):
        return getattr(iterable, "mask", True) if isinstance(iterable, Vectorized) else True

    def contains(self, item, container):
        """``item in container``: elementwise for the sweep's per-instrument containers."""
        if hasattr(container, "sweep_contains"):
            return container.sweep_contains(item)
        if item is ALL:
            raise NotConvertible("`instrument in container` inside a loop over instruments needs a "
                                 "per-instrument dict on the context; a plain list or set holds "
                                 "single instruments.")
        return item in container

    def delete(self, container, key, line):
        if hasattr(container, "sweep_delete"):
            container.sweep_delete(key)
        elif self.plain(self.mask, line, "del"):
            del container[key]

    def comprehension(self, iterable, value, condition, arity, kind):
        if hasattr(iterable, "sweep_vectorized"):
            iterable = iterable.sweep_vectorized()
        from ziplime.sweep.cross import CrossSection, cross_section_of
        vectorized = isinstance(iterable, Vectorized) or type(iterable).__name__ == "VectorUniverse"
        if vectorized:
            items = list(iterable)
            if len(items) == 1:
                item = items[0]
                arguments = item if arity > 1 else (item,)
                mask = self.and_(self.loop_mask(iterable), lambda: condition(*arguments))
                if kind == "dict":
                    return self._dict_of(*value(*arguments), mask)
                return cross_section_of(self.xp, value(*arguments), mask)
        def plain():
            for item in iterable:
                arguments = item if arity > 1 else (item,)
                if self.plain(condition(*arguments), 0, "a comprehension's if"):
                    yield value(*arguments)
        if kind == "dict":
            return self.dict(dict(plain()))
        return list(plain()) if kind == "list" else plain()

    def _dict_of(self, key, value, mask):
        """``{key: value for ... over instruments if ...}``: the entries where the mask holds."""
        from ziplime.sweep.cross import SweepDict, column_mask

        result = SweepDict(self)
        if column_mask(key) is None:
            raise NotConvertible("A dict built over instruments is keyed by the instrument (its "
                                 "name, listing or sid) in a sweep.")
        result._write(key, value, where=mask)
        return result

    def dict(self, value):
        """A dict the strategy builds: one that holds instruments as arrays (see cross.py)."""
        from ziplime.sweep.cross import SweepDict
        return SweepDict(self, value)

    def loop(self, iterable, carried, line):
        if hasattr(iterable, "sweep_vectorized"):
            iterable = iterable.sweep_vectorized()
        if isinstance(iterable, Vectorized) and carried:
            raise NotConvertible(
                f"line {line}: the loop over instruments carries {', '.join(carried)} from one "
                f"instrument to the next. Every instrument is processed at once, so one "
                f"instrument's decision cannot depend on the previous one.")
        return iterable


class _MathShim:
    """``math`` that accepts arrays: math.isnan(signals["close"][name])."""

    def __init__(self, xp):
        self._xp = xp

    def __getattr__(self, name):
        original = getattr(math, name)
        vector = getattr(self._xp, {"fabs": "abs"}.get(name, name), None)
        if not callable(original) or vector is None:
            return original

        def function(*args):
            if any(map(is_array, args)):
                return vector(*args)
            return original(*args)
        return function


#: NumPy functions a converted strategy may call, applied element by element. Each value in
#: the strategy is one number per instrument; in the sweep it is an array of them, one per
#: combination, so every function here has to mean the same thing on that array.
NUMPY_ELEMENTWISE = {
    "abs": "abs", "absolute": "abs", "sign": "sign", "floor": "floor", "ceil": "ceil",
    "round": "round", "around": "round", "sqrt": "sqrt", "square": "square", "exp": "exp",
    "expm1": "expm1", "log": "log", "log1p": "log1p", "log2": "log2", "log10": "log10",
    "sin": "sin", "cos": "cos", "tan": "tan", "tanh": "tanh", "arctan": "arctan",
    "power": "power", "maximum": "maximum", "minimum": "minimum", "fmax": "maximum",
    "fmin": "minimum", "clip": "clip", "where": "where", "isnan": "isnan", "isinf": "isinf",
    "isfinite": "isfinite", "nan_to_num": "nan_to_num", "logical_and": "logical_and",
    "logical_or": "logical_or", "logical_not": "logical_not",
}
#: Reductions: over a list of values (np.mean([a, b, c])) they reduce the list, value by value;
#: over one value they are that value, as np.mean(3.0) is 3.0.
NUMPY_REDUCTIONS = {"mean", "sum", "min", "max", "amin", "amax", "median", "prod", "std", "var",
                    "nanmean", "nansum", "nanmin", "nanmax"}
NUMPY_CONSTANTS = {"nan", "inf", "pi", "e", "NaN", "Inf", "newaxis", "float64", "float32",
                   "int64", "int32", "bool_"}
NUMPY_SUPPORTED = set(NUMPY_ELEMENTWISE) | NUMPY_REDUCTIONS | {"trunc"} | NUMPY_CONSTANTS


class _NumpyShim:
    """``np`` inside a converted strategy: its functions on the backend's arrays."""

    def __init__(self, xp):
        self._xp = xp

    def __getattr__(self, name):
        original = getattr(np, name)
        if name in NUMPY_CONSTANTS or not callable(original):
            return original
        return _HistoryAware(original, self._array_function(name, original), f"np.{name}")

    def _array_function(self, name, original):
        xp = self._xp
        if name == "trunc":
            return lambda value: (xp.sign(value) * xp.floor(abs(value)) if is_array(value)
                                  else np.trunc(value))
        if name in NUMPY_ELEMENTWISE:
            vector = getattr(xp, NUMPY_ELEMENTWISE[name])

            def elementwise(*args, **kwargs):
                if any(map(is_array, args)):
                    return vector(*args, **kwargs)
                return original(*args, **kwargs)
            return elementwise
        if name in NUMPY_REDUCTIONS:
            return lambda values, *args, **kwargs: _reduce(xp, name, original, values, args, kwargs)

        def unsupported(*args, **kwargs):
            raise NotConvertible(f"np.{name} has no counterpart in a sweep on values that differ "
                                 f"between combinations; the supported NumPy functions are "
                                 f"elementwise ones and reductions over a list -- or any of them "
                                 f"on an array of history (data.history(...)['close'].to_numpy()).")
        return unsupported


class _HistoryAware:
    """A function of NumPy or of another library inside converted code: on history rows
    (lazy.py) it is recorded, to run on the host with the real rows; otherwise ``plain``.
    ``np.maximum.accumulate`` and the other attributes of a ufunc are recorded the same way."""

    def __init__(self, original, plain, name):
        self._original, self._plain, self._name = original, plain, name

    def __call__(self, *args, **kwargs):
        from ziplime.sweep.lazy import contains_lazy, record
        if contains_lazy(args, kwargs):
            return record(self._original, args, kwargs)
        return self._plain(*args, **kwargs)

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        attribute = getattr(self._original, name)
        if not callable(attribute):
            return attribute

        def plain(*args, **kwargs):
            raise NotConvertible(f"{self._name}.{name} runs on an array of history only in a "
                                 f"sweep.")
        return _HistoryAware(attribute, plain, f"{self._name}.{name}")


class _Foreign:
    """A module of another library (TA-Lib, SciPy...) inside converted code: its functions on
    history rows are recorded (lazy.py); on values that differ between combinations they cannot
    run, and a call with plain numbers runs as written."""

    def __init__(self, module):
        self._module = module

    def __getattr__(self, name):
        return foreign(getattr(self._module, name), f"{self._module.__name__}.{name}")


def foreign(value, name):
    import types as _types

    if isinstance(value, _types.ModuleType):
        return _Foreign(value)
    if not callable(value) or isinstance(value, type) and issubclass(value, BaseException):
        return value

    def plain(*args, **kwargs):
        from ziplime.sweep.lazy import where_in_strategy
        if any(is_array(a) or a is ALL or hasattr(a, "sweep_len")
               for a in (*args, *kwargs.values())):
            raise NotConvertible(f"{where_in_strategy()}: {name}(...) gets values that differ "
                                 f"between combinations; it computes on the CPU number by number. "
                                 f"On history rows (data.history(...)['close'].to_numpy()) it is "
                                 f"computed ahead of the run; otherwise compute it in "
                                 f"compute_signals.")
        return value(*args, **kwargs)
    return _HistoryAware(value, plain, name)


def _reduce(xp, name, original, values, args, kwargs):
    if args or kwargs.get("axis") is not None:
        raise NotConvertible(f"np.{name} with an axis reduces over combinations in a sweep, not "
                             f"over what the strategy meant; reduce a list: np.{name}([a, b]).")
    ddof = kwargs.get("ddof", 0)
    if isinstance(values, (list, tuple)):
        if not any(map(is_array, values)):
            return original(values, **kwargs)
        shape = np.broadcast_shapes(*[getattr(v, "shape", ()) for v in values])
        base = xp.zeros(shape)
        stacked = xp.stack([v + base for v in values])  # (values, combinations, instruments)
        count = len(values)
        present = xp.logical_not(xp.isnan(stacked))
        clean = xp.where(present, stacked, 0.0)
        if name in ("mean", "sum", "prod", "min", "max", "amin", "amax", "median"):
            target = {"amin": "min", "amax": "max"}.get(name, name)
            return getattr(xp, target)(stacked, axis=0)
        if name in ("std", "var"):
            mean = xp.mean(stacked, axis=0)
            var = xp.sum((stacked - mean) ** 2, axis=0) / max(count - ddof, 1)
            return xp.sqrt(var) if name == "std" else var
        if name == "nansum":
            return xp.sum(clean, axis=0)
        if name == "nanmean":
            return xp.sum(clean, axis=0) / xp.sum(present, axis=0)
        if name in ("nanmin", "nanmax"):
            fill = _RUNTIME_INF[0] if name == "nanmin" else -_RUNTIME_INF[0]
            reduce = xp.min if name == "nanmin" else xp.max
            return reduce(xp.where(present, stacked, fill), axis=0)
    if is_array(values):
        # One number per instrument in the strategy: a reduction of one number is the number
        return values * 0.0 if name in ("std", "var") else values
    return original(values, **kwargs)


#: The infinity of the run in progress (see Runtime.inf), for the NumPy substitute.
_RUNTIME_INF = [np.inf]


def _builtins_shim(xp, runtime=None):
    def scalar(original, vector):
        def function(value=0, *rest):
            return vector(value) if is_array(value) else original(value, *rest)
        return function

    def reduce(original, combine):
        def function(iterable):
            items = list(iterable)
            if not any(map(is_array, items)):
                return original(items)
            result = None
            for item in items:
                item = item if is_array(item) else xp.array(bool(item))
                item = item if item.dtype == getattr(xp, "bool_", np.bool_) else item != 0
                result = item if result is None else combine(result, item)
            return result
        return function

    from ziplime.sweep.lazy import contains_lazy, record

    def history_aware(original, function):
        def call(*args, **kwargs):
            if contains_lazy(args, kwargs):
                return record(original, args, kwargs)
            return function(*args, **kwargs)
        return call

    def length(value):
        return value.sweep_len() if hasattr(value, "sweep_len") else builtins.len(value)

    from ziplime.sweep import cross

    def list_(iterable=()):
        if isinstance(iterable, Vectorized):
            return copy.copy(iterable)  # list(context.universe.values()) is still the universe
        return builtins.list(iterable)

    def sorted_(iterable, *, key=None, reverse=False):
        if cross.ranks_instruments(iterable):
            return cross.ranking(xp, iterable, key, reverse, runtime)
        if runtime is not None:
            ranked = cross.sort_instruments(runtime, iterable, key, reverse)
            if ranked is not None:
                return ranked
        return builtins.sorted(iterable, key=key, reverse=reverse)

    def extreme(name, original, vector):
        def function(*args, key=None, **kwargs):
            if len(args) == 1 and key is not None and cross.ranks_instruments(args[0]):
                return cross.ranking(xp, args[0], key, name == "max", runtime)[0]
            if len(args) == 1 and isinstance(args[0], cross.CrossSection):
                return cross.reduce_across(xp, name, args[0], runtime)
            if len(args) == 2 and not kwargs and key is None and any(map(is_array, args)):
                return vector(*args)
            if key is not None:
                kwargs["key"] = key
            return original(*args, **kwargs)
        return function

    def sum_(iterable, start=0):
        if isinstance(iterable, cross.CrossSection):
            return cross.reduce_across(xp, "sum", iterable, runtime) + start
        return builtins.sum(iterable, start)

    def any_(iterable):
        if isinstance(iterable, cross.CrossSection):
            return cross.reduce_across(xp, "any", iterable, runtime)
        return reduce(builtins.any, xp.logical_or)(iterable)

    def all_(iterable):
        if isinstance(iterable, cross.CrossSection):
            return cross.reduce_across(xp, "all", iterable, runtime)
        return reduce(builtins.all, xp.logical_and)(iterable)

    shims = {
        "len": length,
        "all": all_,
        "any": any_,
        "min": extreme("min", builtins.min, xp.minimum),
        "max": extreme("max", builtins.max, xp.maximum),
        "sorted": sorted_,
        "list": list_,
        "sum": sum_,
        "float": scalar(builtins.float, lambda value: value),
        "int": scalar(builtins.int, lambda value: xp.sign(value) * xp.floor(abs(value))),
        "round": scalar(builtins.round, xp.round),
        "abs": scalar(builtins.abs, xp.abs),
    }
    return {name: history_aware(getattr(builtins, name), shim) for name, shim in shims.items()}


# -- rewriting the source ------------------------------------------------------------------------

M, DEAD, SKIP, RT, RESULT = "_gpu_m", "_gpu_dead", "_gpu_skip", "_gpu_rt", "_gpu_result"


def _template(code: str, **nodes) -> list[ast.stmt]:
    tree = ast.parse(textwrap.dedent(code))
    # The template's own lines (1, 2...) would point at the top of the strategy's function: they
    # take the line of the statement they are generated for instead (block, fix_missing_locations)
    for node in ast.walk(tree):
        for attribute in ("lineno", "col_offset", "end_lineno", "end_col_offset"):
            if hasattr(node, attribute):
                delattr(node, attribute)

    class Fill(ast.NodeTransformer):
        def visit_Name(self, node):
            if node.id in nodes:
                replacement = copy.deepcopy(nodes[node.id])
                if hasattr(replacement, "ctx"):
                    replacement.ctx = node.ctx
                return replacement
            return node

    return Fill().visit(tree).body


def _expression(code: str, **nodes) -> ast.expr:
    return _template(code, **nodes)[0].value


def _load(node: ast.expr) -> ast.expr:
    node = copy.deepcopy(node)
    for child in ast.walk(node):
        if hasattr(child, "ctx"):
            child.ctx = ast.Load()
    return node


class _Expressions(ast.NodeTransformer):
    """and/or/not/if-else/chained comparisons to elementwise; await dropped."""

    def visit_Await(self, node):
        return self.visit(node.value)

    def visit_BoolOp(self, node):
        self.generic_visit(node)
        method = "and_" if isinstance(node.op, ast.And) else "or_"
        result = node.values[0]
        for value in node.values[1:]:
            result = _expression(f"{RT}.{method}(LEFT, lambda: RIGHT)", LEFT=result, RIGHT=value)
        return result

    def visit_UnaryOp(self, node):
        self.generic_visit(node)
        if isinstance(node.op, ast.Not):
            return _expression(f"{RT}.not_(VALUE)", VALUE=node.operand)
        return node

    def visit_IfExp(self, node):
        self.generic_visit(node)
        return _expression(f"{RT}.where(TEST, YES, NO)", TEST=node.test, YES=node.body, NO=node.orelse)

    def visit_Compare(self, node):
        self.generic_visit(node)
        if len(node.ops) == 1:
            op = node.ops[0]
            if isinstance(op, (ast.In, ast.NotIn)):
                test = _expression(f"{RT}.contains(ITEM, CONTAINER)", ITEM=node.left,
                                   CONTAINER=node.comparators[0])
                return test if isinstance(op, ast.In) else _expression(f"{RT}.not_(TEST)", TEST=test)
            if isinstance(op, (ast.Is, ast.IsNot)) and isinstance(node.comparators[0], ast.Constant) \
                    and node.comparators[0].value is None:
                test = _expression(f"{RT}.is_none(VALUE)", VALUE=node.left)
                return test if isinstance(op, ast.Is) else _expression(f"{RT}.not_(TEST)", TEST=test)
            return node
        parts = [ast.Compare(left=left, ops=[op], comparators=[right])
                 for left, op, right in zip([node.left, *node.comparators], node.ops, node.comparators)]
        result = parts[0]
        for part in parts[1:]:
            result = _expression(f"{RT}.and_(LEFT, lambda: RIGHT)", LEFT=result, RIGHT=part)
        return result

    def visit_Lambda(self, node):
        return node  # a lambda's body is not a statement of the strategy

    def visit_Dict(self, node):
        self.generic_visit(node)
        return _expression(f"{RT}.dict(VALUE)", VALUE=node)

    def visit_DictComp(self, node):
        pair = ast.GeneratorExp(elt=ast.Tuple(elts=[node.key, node.value], ctx=ast.Load()),
                                generators=node.generators)
        converted = self._comprehension(pair, kind="dict")
        return _expression(f"{RT}.dict(VALUE)", VALUE=node) if converted is pair else converted

    def visit_GeneratorExp(self, node):
        return self._comprehension(node)

    def visit_ListComp(self, node):
        return self._comprehension(node)

    def _comprehension(self, node, kind=None):
        """``(f(x) for x in xs if c(x))``: over instruments, one array with a mask -- a cross
        section that sum/any/all/max/min/len reduce per combination; otherwise as Python."""
        if len(node.generators) != 1 or node.generators[0].is_async:
            return node
        generator = node.generators[0]
        names = ([generator.target.id] if isinstance(generator.target, ast.Name) else
                 [element.id for element in getattr(generator.target, "elts", [])
                  if isinstance(element, ast.Name)])
        if not names or (not isinstance(generator.target, ast.Name)
                         and len(names) != len(generator.target.elts)):
            return node
        arguments = ast.arguments(posonlyargs=[], args=[ast.arg(arg=n) for n in names],
                                  kwonlyargs=[], kw_defaults=[], defaults=[])
        value = ast.Lambda(args=arguments, body=self.visit(node.elt))
        condition = None
        for test in generator.ifs:
            test = self.visit(test)
            condition = test if condition is None else _expression(
                f"{RT}.and_(LEFT, lambda: RIGHT)", LEFT=condition, RIGHT=test)
        condition = ast.Lambda(args=copy.deepcopy(arguments),
                               body=condition if condition is not None else ast.Constant(True))
        kind = kind or ("list" if isinstance(node, ast.ListComp) else "generator")
        return _expression(f"{RT}.comprehension(ITER, VALUE, CONDITION, {len(names)}, {kind!r})",
                           ITER=self.visit(generator.iter), VALUE=value, CONDITION=condition)


def _is_sort(node) -> bool:
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sort" and not node.args
            and isinstance(node.func.value, (ast.Name, ast.Attribute, ast.Subscript)))


def _carried(loop: ast.For) -> list[str]:
    """What a loop carries from one pass to the next: read before written, or ``+=``."""
    targets = {node.id for node in ast.walk(loop.target) if isinstance(node, ast.Name)}
    events = []
    for statement in loop.body:
        for node in ast.walk(statement):
            if isinstance(node, ast.AugAssign):
                events.append((node.lineno, node.col_offset, "aug", ast.unparse(node.target)))
            elif isinstance(node, ast.Name):
                events.append((node.lineno, node.col_offset,
                               "store" if isinstance(node.ctx, ast.Store) else "load", node.id))
    stored = {name for *_, kind, name in events if kind == "store"} - targets
    carried, seen = [], set()
    # x = f(x) reads x before writing it: the right-hand side comes first
    for line, column, kind, name in sorted(events, key=lambda event: (event[0], event[2] != "load")):
        if kind == "aug" or (kind == "load" and name in stored and name not in seen):
            if name not in carried:
                carried.append(name)
        if kind == "store":
            seen.add(name)
    return carried


class _Statements:
    def __init__(self, filename: str):
        self.filename = filename
        self.counter = 0
        self.expressions = _Expressions()

    def fresh(self, prefix: str) -> str:
        self.counter += 1
        return f"_gpu_{prefix}{self.counter}"

    def expr(self, node):
        return self.expressions.visit(node)

    def refuse(self, node, reason):
        raise NotConvertible(f"{self.filename}, line {node.lineno}: {reason}")

    def block(self, statements) -> list[ast.stmt]:
        result = []
        for statement in statements:
            for new in self.statement(statement):
                result.append(ast.copy_location(new, statement))
                for child in ast.walk(new):
                    if not hasattr(child, "lineno") and "lineno" in child._attributes:
                        ast.copy_location(child, statement)
        return result or [ast.Pass()]

    def assign(self, target, value) -> list[ast.stmt]:
        if isinstance(target, (ast.Tuple, ast.List)):
            temporary = self.fresh("t")
            result = _template(f"{temporary} = VALUE", VALUE=value)
            for index, element in enumerate(target.elts):
                result += self.assign(element, _expression(f"{temporary}[{index}]"))
            return result
        if isinstance(target, ast.Starred):
            self.refuse(target, "starred assignment is not supported.")
        return _template(f"TARGET = {RT}.sel({M}, VALUE, lambda: OLD)",
                         TARGET=target, VALUE=value, OLD=_load(target))

    def statement(self, node) -> list[ast.stmt]:
        mask = _template(f"{RT}.mask = {M}")
        if isinstance(node, ast.Expr) and _is_sort(node.value):
            # lst.sort(key=...) on a list of instruments ranks them per combination
            call = node.value
            receiver = call.func.value
            value = ast.Call(func=_expression(f"{RT}.sort_list"),
                             args=[self.expr(_load(receiver))],
                             keywords=[ast.keyword(arg=k.arg, value=self.expr(k.value))
                                       for k in call.keywords])
            target = copy.deepcopy(receiver)
            target.ctx = ast.Store()
            return mask + self.assign(target, value)
        if isinstance(node, ast.Expr):
            return mask + [ast.Expr(self.expr(node.value))]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return self.function(node)
        if isinstance(node, ast.Assign):
            value = self.expr(node.value)
            if len(node.targets) == 1:
                return mask + self.assign(node.targets[0], value)
            temporary = self.fresh("t")
            result = mask + _template(f"{temporary} = VALUE", VALUE=value)
            for target in node.targets:
                result += self.assign(target, _expression(temporary))
            return result
        if isinstance(node, ast.AnnAssign):
            return mask + self.assign(node.target, self.expr(node.value)) if node.value else []
        if isinstance(node, ast.AugAssign):
            value = ast.BinOp(left=_load(node.target), op=node.op, right=self.expr(node.value))
            return mask + self.assign(node.target, value)
        if isinstance(node, ast.Return):
            result = []
            if node.value is not None:
                result = mask + _template(f"{RESULT} = {RT}.returned({M}, VALUE, {RESULT})",
                                          VALUE=self.expr(node.value))
            return result + _template(f"""
                {DEAD} = {RT}.or_({DEAD}, lambda: {M})
                if {DEAD} is True:
                    return {RESULT}
                {M} = False
            """)
        if isinstance(node, ast.Continue):
            return _template(f"""
                {SKIP} = {RT}.or_({SKIP}, lambda: {M})
                {M} = False
            """)
        if isinstance(node, ast.Break):
            return _template(f"""
                if {RT}.plain({M}, {node.lineno}, "break"):
                    break
            """)
        if isinstance(node, ast.If):
            return self.if_(node)
        if isinstance(node, (ast.For, ast.AsyncFor)):
            return self.for_(node)
        if isinstance(node, ast.While):
            return self.while_(node)
        if isinstance(node, ast.Raise):
            return [ast.If(test=_expression(f'{RT}.plain({M}, {node.lineno}, "raise")'),
                           body=[node], orelse=[])]
        if isinstance(node, (ast.Pass, ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal)):
            return [node]
        if isinstance(node, ast.Delete):
            result = []
            for target in node.targets:
                if not isinstance(target, ast.Subscript):
                    self.refuse(node, "del of anything but a container's item is not supported.")
                result += mask + [ast.Expr(_expression(
                    f"{RT}.delete(CONTAINER, KEY, {node.lineno})",
                    CONTAINER=self.expr(_load(target.value)), KEY=self.expr(_load(target.slice))))]
            return result
        if isinstance(node, ast.Assert):
            return [ast.Assert(test=_expression(f'{RT}.plain(TEST, {node.lineno}, "assert")',
                                                TEST=self.expr(node.test)), msg=node.msg)]
        names = {ast.Try: "try", ast.With: "with", ast.AsyncWith: "async with",
                 ast.FunctionDef: "a nested function", ast.AsyncFunctionDef: "a nested function",
                 ast.ClassDef: "a class"}
        self.refuse(node, f"{names.get(type(node), type(node).__name__)} is not supported in a "
                          f"function the sweep runs for every combination.")

    def function(self, node) -> list[ast.stmt]:
        """A nested function: converted like the one around it, and run under the mask of the
        statement that calls it."""
        if node.decorator_list:
            self.refuse(node, "a decorated nested function is not supported.")
        if any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in ast.walk(node)):
            self.refuse(node, "a generator is not supported in a function the sweep runs for every "
                              "combination.")
        inner = self.fresh("f")
        body = _template(f"""
            {M} = {RT}.mask
            {DEAD} = False
            {SKIP} = False
            {RESULT} = None
        """) + self.block(node.body) + _template(f"return {RESULT}")
        arguments = copy.deepcopy(node.args)
        for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs,
                         arguments.vararg, arguments.kwarg):
            if argument is not None:
                argument.annotation = None
        arguments.defaults = [self.expr(d) for d in arguments.defaults]
        arguments.kw_defaults = [self.expr(d) if d is not None else None
                                 for d in arguments.kw_defaults]
        definition = ast.FunctionDef(name=inner, args=arguments, body=body, decorator_list=[],
                                     returns=None, type_params=[])
        return [definition] + _template(f"{node.name} = {RT}.nested({inner})")

    def if_(self, node: ast.If) -> list[ast.stmt]:
        condition, saved = self.fresh("c"), self.fresh("s")
        result = _template(f"""
            {condition} = {RT}.truth(TEST)
            {saved} = {M}
            {M} = {RT}.and_({saved}, lambda: {condition})
        """, TEST=self.expr(node.test))
        result.append(ast.If(test=_expression(f"{M} is not False"), body=self.block(node.body),
                             orelse=[]))
        if node.orelse:
            result += _template(f"""
                {M} = {RT}.and_({RT}.and_({saved}, lambda: {RT}.not_({condition})),
                                lambda: {RT}.alive({DEAD}, {SKIP}))
            """)
            result.append(ast.If(test=_expression(f"{M} is not False"),
                                 body=self.block(node.orelse), orelse=[]))
        result += _template(f"{M} = {RT}.and_({saved}, lambda: {RT}.alive({DEAD}, {SKIP}))")
        return result

    def for_(self, node) -> list[ast.stmt]:
        if node.orelse:
            self.refuse(node, "for ... else is not supported.")
        skip, path = self.fresh("k"), self.fresh("p")
        carried = _carried(node)
        result = _template(f"""
            {skip} = {SKIP}
            {path} = {M}
        """)
        items = self.fresh("i")
        result += _template(f"{items} = {RT}.loop(ITER, {carried!r}, {node.lineno})",
                            ITER=self.expr(node.iter))
        body = _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
            {M} = {RT}.and_({M}, lambda: {RT}.loop_mask({items}))
        """) + [ast.If(test=_expression(f"{M} is not False"), body=self.block(node.body), orelse=[])]
        result.append(ast.For(target=node.target, iter=ast.Name(items, ast.Load()), body=body,
                              orelse=[]))
        result += _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
        """)
        return result

    def while_(self, node: ast.While) -> list[ast.stmt]:
        if node.orelse:
            self.refuse(node, "while ... else is not supported.")
        skip, path = self.fresh("k"), self.fresh("p")
        result = _template(f"""
            {skip} = {SKIP}
            {path} = {M}
        """)
        test = _expression(f'{RT}.plain({RT}.and_({M}, lambda: TEST), {node.lineno}, "while")',
                           TEST=self.expr(node.test))
        body = self.block(node.body) + _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
        """)
        result.append(ast.While(test=test, body=body, orelse=[]))
        result += _template(f"{M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))")
        return result


def _source(function) -> tuple[ast.FunctionDef | ast.AsyncFunctionDef, str, int]:
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError):
        raise NotConvertible(f"The source of {function.__name__} is not available.") from None
    filename = inspect.getsourcefile(function) or f"<{function.__name__}>"
    first_line = inspect.getsourcelines(function)[1]
    tree = ast.parse(source)
    definition = tree.body[0]
    if not isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise NotConvertible(f"{function.__name__} is not a function.")
    return definition, filename, first_line


def _compile(function) -> types.FunctionType:
    """The converted function; it takes the Runtime as an extra last argument."""
    definition, filename, first_line = _source(function)
    statements = _Statements(filename)
    body = _template(f"""
        {M} = {RT}.mask
        {DEAD} = False
        {SKIP} = False
        {RESULT} = None
    """) + statements.block(definition.body) + _template(f"return {RESULT}")
    arguments = copy.deepcopy(definition.args)
    # Annotations name types the module may only import for type checking
    for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs,
                     arguments.vararg, arguments.kwarg):
        if argument is not None:
            argument.annotation = None
    arguments.args.append(ast.arg(arg=RT))
    new = ast.FunctionDef(name=definition.name, args=arguments, body=body, decorator_list=[],
                          returns=None, type_params=[])
    module = ast.Module(body=[ast.copy_location(new, definition)], type_ignores=[])
    ast.increment_lineno(module, first_line - 1)
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, filename, "exec"), namespace)
    return namespace[definition.name]


#: Module functions that are the strategy's hooks, not helpers it calls.
HOOKS = {"initialize", "compute_signals", "handle_data_gpu", "analyze"}


def user_module(module) -> bool:
    """A module of the strategy's own code: not the standard library, an installed package or
    ziplime. Its functions are converted like the strategy's."""
    import sysconfig

    if isinstance(module, str):
        module = sys.modules.get(module)
    path = getattr(module, "__file__", None)
    if not path:
        return False
    path = os.path.realpath(path)
    if module.__name__ == "ziplime.ziplime_algorithm":
        return True  # the strategy file, loaded by AlgorithmFile under this name
    roots = {sysconfig.get_paths()[key] for key in ("stdlib", "platstdlib", "purelib", "platlib")}
    roots.add(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))  # ziplime itself
    if any(path.startswith(os.path.realpath(root) + os.sep) for root in roots if root):
        return False
    return "site-packages" not in path and "dist-packages" not in path


class Converted:
    """A converted hook and the functions of the strategy's own modules it may call."""

    def __init__(self, original, function, helpers: dict, helper_originals: dict):
        self.original = original
        self.function = function
        #: {(module, name): converted code} for functions of the strategy's own modules
        self.helpers = helpers
        self.helper_originals = helper_originals
        functions = [original, *helper_originals.values()]
        self.source = "\n".join(_text(function) for function in functions)
        #: Whether limit or stop prices appear: the simulation then tracks triggers
        self.priced_orders = any(word in self.source for word in
                                 ("LimitOrder", "StopOrder", "limit_price", "stop_price"))
        #: context attributes the functions assign, and why a compiled step cannot carry them
        self.writes, self.in_place, self.compile_blockers = context_writes(functions)

    @property
    def name(self) -> str:
        return self.original.__name__

    def bind(self, xp, runtime: Runtime | None = None):
        """The function ``(context, data)`` for one run: its own Runtime, and for each module of
        the strategy's code its own globals, with math, NumPy and builtins substituted and its
        functions replaced by their converted versions."""
        runtime = runtime or Runtime(xp)
        from ziplime.sweep.frames import PolarsShim

        numpy, maths, polars = _NumpyShim(xp), _MathShim(xp), PolarsShim()
        builtins_ = {**vars(builtins), **_builtins_shim(xp, runtime)}
        namespaces = {}

        def wrapper(module_name, name):
            code = self.helpers[(module_name, name)]
            original = self.helper_originals[(module_name, name)]
            converted = types.FunctionType(code.__code__, namespace_of(original.__module__,
                                                                       original.__globals__),
                                           name, original.__defaults__)

            def call_helper(*args, **kwargs):
                saved = runtime.mask
                try:
                    return converted(*args, runtime, **kwargs)
                finally:
                    runtime.mask = saved
            return call_helper

        def substitute(value):
            if value is math:
                return maths
            if value is np:
                return numpy
            if getattr(value, "__name__", None) == "polars" and isinstance(value, types.ModuleType):
                return polars
            if callable(value) and getattr(value, "__name__", None) == "col" and \
                    (module or "").split(".")[0] == "polars":
                return polars.col
            module = getattr(value, "__module__", None)
            if callable(value) and module == "math":
                return getattr(maths, value.__name__)
            if callable(value) and module == "numpy" and getattr(value, "__name__", None) in NUMPY_SUPPORTED:
                return getattr(numpy, value.__name__)
            if isinstance(value, types.FunctionType) and (module, value.__name__) in self.helpers:
                return wrapper(module, value.__name__)
            if isinstance(value, types.ModuleType) and user_module(value):
                return _ModuleProxy(value, namespace_of(value.__name__, vars(value)))
            if _is_foreign(value):
                return foreign(value, getattr(value, "__name__", "it"))
            return value

        def namespace_of(module_name, original_globals):
            if module_name not in namespaces:
                namespace = namespaces[module_name] = {}
                namespace.update(original_globals)
                for name, value in list(original_globals.items()):
                    namespace[name] = substitute(value)
                namespace["__builtins__"] = builtins_
            return namespaces[module_name]

        namespace = namespace_of(self.original.__module__, self.original.__globals__)
        function = types.FunctionType(self.function.__code__, namespace, self.function.__name__,
                                      self.function.__defaults__)

        def call(context, data, mask=True):
            """``mask``: where the hook runs -- a scheduled function's rule in a compiled step."""
            runtime.mask = mask
            return function(context, data, runtime)
        call.runtime = runtime
        call.namespace = namespace
        return call


def _is_foreign(value) -> bool:
    """A module or function of another library: not Python's, NumPy, polars or ziplime."""
    from ziplime.sweep.check import ALLOWED_MODULES

    if isinstance(value, types.ModuleType):
        module = value.__name__
    elif callable(value) and not isinstance(value, type):
        module = getattr(value, "__module__", None) or ""
    else:
        return False
    top = module.split(".")[0]
    return bool(top) and top not in ALLOWED_MODULES and top not in ("numpy", "polars", "math") \
        and not user_module(module)


class _ModuleProxy:
    """``import mylib`` in the strategy: ``mylib.helper`` is the converted helper."""

    def __init__(self, module, namespace):
        self._module, self._namespace = module, namespace

    def __getattr__(self, name):
        if name in self._namespace:
            return self._namespace[name]
        return getattr(self._module, name)


def convert(function) -> Converted:
    """A strategy hook ``(context, data)`` for every combination at once.

    Raises NotConvertible, naming the line, when it cannot be. Functions it reaches in the
    strategy's own modules -- the strategy file and modules it imports from its folder -- are
    converted too, as are the functions those reach.
    """
    converted = _compile(function)
    helpers, originals = {}, {}
    seen_modules = set()

    def visit(namespace: dict):
        for name, value in list(namespace.items()):
            if isinstance(value, types.ModuleType) and user_module(value) \
                    and value.__name__ not in seen_modules:
                seen_modules.add(value.__name__)
                visit(vars(value))
                continue
            if not isinstance(value, types.FunctionType) or not (
                    value.__module__ == function.__module__ or user_module(value.__module__)):
                continue
            key = (value.__module__, value.__name__)
            if key in helpers or value is function or value.__name__ in HOOKS \
                    or value.__name__.startswith("handle_data") \
                    or value.__name__ == "before_trading_start":
                continue
            try:
                helpers[key] = _compile(value)
                originals[key] = value
            except NotConvertible:
                continue  # left as it is; it fails only if called with arrays
            visit(value.__globals__)

    visit(function.__globals__)
    return Converted(function, converted, helpers, originals)


def _text(function) -> str:
    try:
        return inspect.getsource(function)
    except (OSError, TypeError):
        return ""


# -- what a function writes to the context ----------------------------------------------------------

MUTATING = {"append", "extend", "update", "pop", "insert", "add", "clear", "remove", "setdefault",
            "popitem", "discard", "sort", "reverse"}


def context_writes(functions) -> tuple[set[str], set[str], list[str]]:
    """``context.X`` attributes the functions assign, those they change in place
    (``context.X[k] = v``, ``del``, ``.pop()``), and reasons the step cannot be compiled.

    A compiled step is traced once, so Python state that changes from bar to bar has to travel as
    arrays. Numbers assigned to ``context.X`` can, and so can a per-instrument dict; other
    containers changed in place and values that vary with the date cannot.
    """
    written, in_place, reasons = set(), set(), []

    def owner(node):
        """``context.X[...]`` / ``context.X.pop`` -> "X"."""
        while isinstance(node, (ast.Subscript, ast.Attribute)):
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id == context):
                return node.attr
            node = node.value
        return None

    for function in functions:
        try:
            definition, filename, first_line = _source(function)
        except NotConvertible:
            continue
        context = definition.args.args[0].arg if definition.args.args else "context"
        for node in ast.walk(definition):
            line = first_line + getattr(node, "lineno", 1) - 1
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
                targets = [node.target]
            elif isinstance(node, ast.Delete):
                targets = node.targets
            for target in targets:
                if (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                        and target.value.id == context):
                    written.add(target.attr)
                elif isinstance(target, ast.Subscript) and owner(target.value):
                    in_place.add(owner(target.value))
                elif isinstance(target, ast.Subscript) and context in ast.unparse(target.value):
                    reasons.append(f"line {line}: {ast.unparse(target)} is changed in place")
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in MUTATING and owner(node.func.value)):
                in_place.add(owner(node.func.value))
            if (isinstance(node, ast.Attribute) and node.attr in ("get_datetime", "current_dt",
                                                                 "current_session")):
                reasons.append(f"line {line}: reads the date")
    return written, in_place, reasons
