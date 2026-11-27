"""Converted for convert."""

import builtins
import math
import types
from collections.abc import Callable
from types import ModuleType
import numpy as np
from ziplime.sweep.api import Context, Data
from ziplime.sweep.convert.support import NUMPY_SUPPORTED
from ziplime.sweep.convert.runtime import Runtime
from ziplime.sweep.convert.math_shim import _MathShim
from ziplime.sweep.convert.module_proxy import _ModuleProxy
from ziplime.sweep.convert.numpy_shim import _NumpyShim
from ziplime.sweep.convert.builtin_utils import _builtins_shim
from ziplime.sweep.convert.foreign_utils import _is_foreign
from ziplime.sweep.convert.source_utils import _text
from ziplime.sweep.convert.source_utils import context_writes
from ziplime.sweep.convert.foreign_utils import foreign
from ziplime.sweep.convert.foreign_utils import user_module


class Converted:
    """A converted hook and the functions of the strategy's own modules it may call."""

    def __init__(self, original: Callable, function: Callable, helpers: dict, helper_originals: dict) -> None:
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

    def bind(self, xp: ModuleType, runtime: Runtime | None = None):
        """The function ``(context, data)`` for one run: its own Runtime, and for each module of
        the strategy's code its own globals, with math, NumPy and builtins substituted and its
        functions replaced by their converted versions."""
        runtime = runtime or Runtime(xp)
        from ziplime.sweep.frames import PolarsShim

        numpy, maths, polars = _NumpyShim(xp), _MathShim(xp), PolarsShim()
        builtins_ = {**vars(builtins), **_builtins_shim(xp, runtime)}
        namespaces = {}

        def wrapper(module_name: str, name: str):
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

        def namespace_of(module_name: str, original_globals):
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

        def call(context: Context, data: Data, mask=True):
            """``mask``: where the hook runs -- a scheduled function's rule in a compiled step."""
            runtime.mask = mask
            return function(context, data, runtime)
        call.runtime = runtime
        call.namespace = namespace
        return call
