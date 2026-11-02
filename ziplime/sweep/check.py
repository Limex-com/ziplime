"""What a strategy calls, checked against what a sweep provides -- before the first bar.

A sweep must never quietly produce different numbers from the engine. Whatever the strategy reads
or calls on ``context``, ``context.portfolio`` and ``data`` that the sweep does not reproduce is
reported with its line, all of it at once, before anything is computed.
"""
import ast
import inspect
import textwrap

from ziplime.sweep.params import SweepError

CONTEXT = {
    "algorithm", "signals", "universe", "portfolio", "xp",
    "order", "order_value", "order_percent", "order_target", "order_target_value",
    "order_target_percent", "get_open_orders", "get_datetime", "record", "rebalance",
    "symbol", "symbols",
}
PORTFOLIO = {"cash", "portfolio_value", "positions_value", "positions_exposure",
             "get_asset_positions_amount", "positions"}
DATA = {"current", "history", "can_trade", "current_dt", "returns"}

HINTS = {
    "cancel_order": "orders cannot be cancelled one by one in a sweep; set_cancel_policy(EODCancel()) "
                    "in initialize is supported",
    "schedule_function": "call it in initialize",
    "account": "context.account is not simulated; portfolio.portfolio_value and cash are",
}


def problems(functions, state_names: set[str]) -> list[str]:
    """Every unsupported use in ``functions``, as ``file, line N: ...``."""
    found = []
    for function in functions:
        try:
            source = textwrap.dedent(inspect.getsource(function))
        except (OSError, TypeError):
            continue
        filename = inspect.getsourcefile(function) or function.__name__
        first = inspect.getsourcelines(function)[1]
        tree = ast.parse(source)
        definition = tree.body[0]
        arguments = [a.arg for a in definition.args.args]
        context = arguments[0] if arguments else "context"
        data = arguments[1] if len(arguments) > 1 else "data"
        written = {node.attr for node in ast.walk(definition)
                   if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store)
                   and isinstance(node.value, ast.Name) and node.value.id == context}
        allowed = CONTEXT | state_names | written
        for node in ast.walk(definition):
            if not isinstance(node, ast.Attribute) or not isinstance(node.ctx, ast.Load):
                continue
            line = first + node.lineno - 1
            owner = node.value
            if isinstance(owner, ast.Name) and owner.id == context and node.attr not in allowed:
                found.append(_say(filename, line, f"context.{node.attr}", node.attr))
            elif (isinstance(owner, ast.Attribute) and owner.attr == "portfolio"
                  and isinstance(owner.value, ast.Name) and owner.value.id == context
                  and node.attr not in PORTFOLIO):
                found.append(_say(filename, line, f"context.portfolio.{node.attr}", node.attr))
            elif isinstance(owner, ast.Name) and owner.id == data and node.attr not in DATA:
                found.append(_say(filename, line, f"data.{node.attr}", node.attr))
    return found


def _say(filename, line, what, name):
    hint = HINTS.get(name)
    return f"{filename}, line {line}: {what} is not available in a sweep" + (f" -- {hint}" if hint else "")


#: Modules whose functions a converted strategy may call: Python's own, NumPy through the sweep's
#: substitute (the functions listed in convert.NUMPY_SUPPORTED), and ziplime's API.
ALLOWED_MODULES = {"builtins", "math", "datetime", "collections", "itertools", "functools",
                   "operator", "typing", "enum", "dataclasses", "ziplime"}


def _module_of(value) -> str:
    if inspect.ismodule(value):
        return value.__name__
    module = getattr(value, "__module__", None)
    if module is None or not (inspect.isfunction(value) or inspect.isclass(value)
                              or inspect.isbuiltin(value) or callable(value) and hasattr(value, "__name__")):
        module = type(value).__module__
    return module or ""


def _chain(node):
    """``a.b.c`` -> ("a", ["b", "c"]); None for anything that does not start with a name."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    return (node.id, list(reversed(parts))) if isinstance(node, ast.Name) else None


def _reads(node, names: set[str], data: str) -> bool:
    """Whether a call's arguments read history: a name holding history rows, or data.history."""
    for argument in [*node.args, *(k.value for k in node.keywords)]:
        for child in ast.walk(argument):
            if isinstance(child, ast.Name) and child.id in names:
                return True
            if isinstance(child, ast.Attribute) and child.attr == "history" and \
                    isinstance(child.value, ast.Name) and child.value.id == data:
                return True
    return False


def _history_names(definition, data: str) -> set[str]:
    """Names that hold history rows or what is computed from them: assigned from data.history
    or from such a name, or filled with them (``prices[s] = df["close"].to_numpy()``)."""
    names = set()
    while True:
        before = len(names)
        for node in ast.walk(definition):
            if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)) and node.value is not None:
                source = ast.Call(func=ast.Name("f"), args=[node.value], keywords=[])
                if not _reads(source, names, data):
                    continue
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for target in targets:
                    for child in ast.walk(target):
                        if isinstance(child, ast.Name):
                            names.add(child.id)
                            break
        if len(names) == before:
            return names


def foreign_calls(functions, state: dict) -> list[str]:
    """Calls into code the sweep cannot run for every combination: other libraries, objects of
    other libraries kept on the context, functions of the strategy's other modules.

    Such code runs on the CPU number by number. Handed an array of a hundred thousand
    combinations it either fails or -- worse -- quietly computes something else.
    """
    from ziplime.sweep.convert import NUMPY_SUPPORTED, user_module

    found = []
    for function in functions:
        try:
            source = textwrap.dedent(inspect.getsource(function))
        except (OSError, TypeError):
            continue
        filename = inspect.getsourcefile(function) or function.__name__
        first = inspect.getsourcelines(function)[1]
        definition = ast.parse(source).body[0]
        arguments = [a.arg for a in definition.args.args]
        context = arguments[0] if arguments else "context"
        data = arguments[1] if len(arguments) > 1 else "data"
        own_module = function.__module__
        namespace = function.__globals__
        history = _history_names(definition, data)
        for node in ast.walk(definition):
            if not isinstance(node, ast.Call):
                continue
            chain = _chain(node.func)
            if chain is None:
                continue
            root, parts = chain
            line = first + node.lineno - 1
            text = ast.unparse(node.func)
            if root == data:
                continue
            if _reads(node, history, data):
                from ziplime.sweep.lazy import CHANGING, is_object_state
                on_state = root == context and parts and parts[0] in state
                if not on_state or (is_object_state(state[parts[0]]) and parts[-1] not in CHANGING):
                    continue  # computed from history rows on the host, before the run (lazy.py)
            if root == context:
                if not parts or parts[0] not in state:
                    continue  # the context API; check() reports what it does not provide
                value = state[parts[0]]
                for part in parts[1:-1]:
                    value = getattr(value, part, None)
            elif root in namespace:
                value = namespace[root]
                for part in parts[:-1]:
                    if not inspect.ismodule(value):
                        break
                    value = getattr(value, part, None)
            else:
                continue  # a local variable, a builtin or a value the strategy computed
            module = _module_of(value)
            top = module.split(".")[0]
            if module == own_module or top in ALLOWED_MODULES or user_module(module):
                continue  # the strategy's own code is converted with it
            if top == "polars":
                name = parts[-1] if parts else getattr(value, "__name__", root)
                if name == "col":
                    continue  # pl.col: the sweep's own, for reading a frame of several instruments
                found.append(f"{filename}, line {line}: {text}(...) -- polars' {name} is not "
                             f"supported in a sweep; a frame from data.history is read with "
                             f"pl.col(...): filter(pl.col('sid') == asset.sid) or "
                             f"group_by('sid').agg(pl.col('close').mean())")
                continue
            if top == "numpy":
                name = parts[-1] if parts else getattr(value, "__name__", root)
                if (inspect.ismodule(value) and len(parts) == 1 and name in NUMPY_SUPPORTED) or \
                        (not inspect.ismodule(value) and name in NUMPY_SUPPORTED):
                    continue
                found.append(f"{filename}, line {line}: {text}(...) -- NumPy's {name} has no "
                             f"elementwise counterpart in a sweep (supported: the elementwise "
                             f"functions and reductions over a list, e.g. np.log, np.where, "
                             f"np.mean([a, b]))")
                continue
            found.append(
                f"{filename}, line {line}: {text}(...) calls {module}, which computes on the CPU "
                f"number by number and cannot run for every combination at once. If it computes "
                f"from price history (an indicator, a model's prediction), compute it in "
                f"compute_signals or context.indicators and read the result from context.signals "
                f"in handle_data; if it needs the portfolio at every bar, the strategy cannot be "
                f"swept.")
    return found



def containers(hooks, state: dict, universe: dict) -> list[str]:
    """Containers on the context changed in place that are not per-instrument dicts of numbers.

    ``context.entry[asset] = price`` is held as arrays; ``context.seen.append(x)`` or a dict of
    dates is a Python object that cannot differ between combinations.
    """
    from ziplime.sweep.api import Columns, asset_map

    class _Backend:
        xp = None

    columns = Columns.__new__(Columns)
    columns.names = list(universe)
    columns.by_name = {name: k for k, name in enumerate(columns.names)}
    columns.by_asset = {listing: k for k, listing in enumerate(universe.values())}
    columns.by_sid = {getattr(listing, "sid", None): k for k, listing in enumerate(universe.values())}
    found = []
    for hook in hooks:
        for name in sorted(hook.in_place):
            if not asset_map(state.get(name, {}), columns):
                found.append(
                    f"{hook.original.__name__}: context.{name} is changed in place but is not a dict "
                    f"of numbers keyed by the universe's instruments; that is the container a sweep "
                    f"can hold for every combination (store a bar count instead of a date).")
    return found


def check(functions, state_names: set[str], state: dict | None = None, hooks=(), universe=None):
    found = problems(functions, state_names) + foreign_calls(functions, state or {})
    if universe is not None:
        found += containers(hooks, state or {}, universe)
    if found:
        raise SweepError("The strategy uses what a sweep does not simulate, so its numbers would "
                         "differ from the engine's:\n  " + "\n  ".join(dict.fromkeys(found)))
