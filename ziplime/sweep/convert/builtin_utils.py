"""Builtin utils for convert."""

import builtins
import copy
from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.vectorized_containers import Vectorized
from ziplime.sweep.convert.runtime import Runtime


def _builtins_shim(xp: ModuleType, runtime: Runtime | None = None):
    def scalar(original: Callable, vector: Callable):
        def function(value=0, *rest):
            return vector(value) if is_array(value) else original(value, *rest)
        return function

    def reduce(original: Callable, combine: Callable):
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

    def history_aware(original: Callable, function: Callable):
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

    def sorted_(iterable, *, key: Callable | None = None, reverse: bool = False):
        if cross.ranks_instruments(iterable):
            return cross.ranking(xp, iterable, key, reverse, runtime)
        if runtime is not None:
            ranked = cross.sort_instruments(runtime, iterable, key, reverse)
            if ranked is not None:
                return ranked
        return builtins.sorted(iterable, key=key, reverse=reverse)

    def extreme(name: str, original: Callable, vector: Callable):
        def function(*args, key: Callable | None = None, **kwargs):
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
