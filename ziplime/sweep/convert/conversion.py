"""Conversion for convert."""

import types
from collections.abc import Callable
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.converted import Converted
from ziplime.sweep.convert.support import HOOKS
from ziplime.sweep.convert.compiler import _compile
from ziplime.sweep.convert.foreign_utils import user_module


def convert(function: Callable) -> Converted:
    """A strategy hook ``(context, data)`` for every combination at once.

    Raises NotConvertible, naming the line, when it cannot be. Functions it reaches in the
    strategy's own modules -- the strategy file and modules it imports from its folder -- are
    converted too, as are the functions those reach.
    """
    converted = _compile(function)
    helpers, originals = {}, {}
    seen_modules = set()

    def visit(namespace: dict) -> None:
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
