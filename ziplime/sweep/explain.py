"""What a sweep did with the strategy, and what in it looks wrong -- printed with the results.

How a strategy runs in a sweep is not visible from outside: which hooks were converted, whether
the bars were compiled and why not, which computations on history were done ahead of the run and
how. :func:`conversion` says it. Two kinds of mistakes are only visible once the sweep has run,
and it says those too: a computation on history that is NaN at every bar (a branch it decides never
runs -- ``talib.RSI(p[-30:], timeperiod=30)`` has no value on 30 rows), and a parameter that changes
nothing (:func:`influence`).
"""
import operator
import os

import pandas as pd

_OPERATORS = {operator.add: "+", operator.sub: "-", operator.mul: "*", operator.truediv: "/",
              operator.floordiv: "//", operator.mod: "%", operator.pow: "**",
              operator.lt: "<", operator.le: "<=", operator.gt: ">", operator.ge: ">=",
              operator.eq: "==", operator.ne: "!="}
LONGEST = 110
#: Computations on history listed one by one; the rest are counted
SHOWN = 30


def describe(key, columns: list) -> str:
    """A record's key as the code it stands for, shortened."""
    text = _describe(key, columns)
    return text if len(text) <= LONGEST else text[:LONGEST - 3] + "..."


def _describe(key, columns) -> str:
    tag = key[0]
    if tag == "leaf":
        _, where, field, length, before, daily = key
        instrument = "each instrument" if where == "all" else columns[where]
        return (f"history({instrument}, {field}, {_describe(length, columns)} "
                f"{'days' if daily else 'bars'}{', before the open' if before else ''})")
    if tag == "param":
        return "<config>"
    if tag == "value":
        return repr(key[2])
    if tag == "nan":
        return "nan"
    if tag == "slice":
        parts = ["" if part == ("value", "NoneType", None) else _describe(part, columns)
                 for part in key[1:]]
        return ":".join(parts[:2]) + (f":{parts[2]}" if parts[2] else "")
    if tag in ("list", "tuple"):
        items = ", ".join(_describe(item, columns) for item in key[1])
        return f"[{items}]" if tag == "list" else f"({items})"
    if tag == "attr":
        return f"{_describe(key[1], columns)}.{key[2]}"
    if tag == "function":
        return getattr(key[1], "__name__", "a function")
    if tag != "call":
        return "..."
    function, args, kwargs = key[1], key[2], key[3]
    shown = [_describe(arg, columns) for arg in args] + \
        [f"{name}={_describe(arg, columns)}" for name, arg in kwargs]
    if function is operator.getitem:
        return f"{shown[0]}[{shown[1]}]"
    if function in _OPERATORS and len(shown) == 2:
        return f"({shown[0]} {_OPERATORS[function]} {shown[1]})"
    if function is operator.neg:
        return f"-{shown[0]}"
    if type(function).__name__ == "_ObjectCall":
        return f"{function.__name__}({', '.join(shown[1:])})"
    name = getattr(function, "name", None) or getattr(function, "__name__", "f")
    if type(function).__name__ == "_Method":
        if name == "to_numpy" and len(shown) == 1:
            return shown[0]
        return f"{shown[0]}.{name}({', '.join(shown[1:])})"
    module = (getattr(function, "__module__", None) or "").split(".")[0]
    prefix = {"numpy": "np.", "talib": "talib.", "builtins": "", "operator": ""}.get(module, "")
    return f"{prefix}{name}({', '.join(shown)})"


def conversion(*, hooks, windows, backend: str, compiled: bool, reasons, columns) -> list[str]:
    """How the strategy ran: lines for the sweep's output."""
    lines = ["How the strategy runs in the sweep:"]
    names = [hook.name for hook in hooks.all()]
    where = "the CPU in float64" if backend in ("exact", "cpu") else f"the GPU ({backend})"
    lines.append(f"  {', '.join(names)}: converted -- every combination at once, on {where}")
    if backend == "mlx":
        if compiled:
            lines.append("  bars: compiled, several bars per call")
        else:
            why = "; ".join(dict.fromkeys(reasons or [])) or "the backend does not compile"
            lines.append(f"  bars: not compiled -- {why}")
    for note in sorted(getattr(windows, "notes", ())):
        lines.append(f"  note: {note}")
    statistics = sum(len(lengths) for lengths in windows.lengths.values())
    if statistics:
        lines.append(f"  data.history statistics (.mean(), .std()...): {statistics} computed "
                     f"before the run")
    registry = windows.lazy
    if registry.records:
        lines.append(f"  computations on history, done before the run ({len(registry.records)}):")
        shown = list(registry.records.items())
        for key, (name, _, _) in shown[:SHOWN]:
            values = len(registry.values.get(name, [()]))
            count = f", {values} values of the config" if values > 1 else ""
            lines.append(f"    {_where(registry.where.get(name))}{describe(key, columns)}"
                         f" -- {registry.how(key)}{count}")
        if len(shown) > SHOWN:
            hows = {}
            for key, _ in shown[SHOWN:]:
                hows[registry.how(key)] = hows.get(registry.how(key), 0) + 1
            lines.append(f"    ... and {len(shown) - SHOWN} more: " +
                         ", ".join(f"{count} {how}" for how, count in hows.items()))
    return lines


def warnings(windows, columns) -> list[str]:
    """Computations on history that are NaN at every bar."""
    registry, found = windows.lazy, []
    for key, (name, _, _) in registry.records.items():
        if registry.always_nan(key):
            found.append(f"  {_where(registry.where.get(name))}{describe(key, columns)} is NaN at "
                         f"every bar: every comparison with it is False, so what it decides never "
                         f"happens. TA-Lib needs more rows than the period (RSI and ROC "
                         f"period + 1); is the window long enough?")
    return found


def influence(table: pd.DataFrame, parameters: list[str]) -> list[str]:
    """Parameters that change nothing: every set of combinations that differ in it alone has
    the same fills and final value."""
    found = []
    frame = table[[p for p in parameters if p in table] + ["fills"]].assign(
        value=table["final_value"].round(2))
    for parameter in parameters:
        if parameter not in frame or frame[parameter].nunique() < 2:
            continue
        others = [p for p in parameters if p != parameter and p in frame]
        if others:
            counts = frame.groupby(others, dropna=False)[[parameter, "fills", "value"]].nunique()
        else:
            counts = frame[[parameter, "fills", "value"]].nunique().to_frame().T
        compared = counts[counts[parameter] > 1]
        if len(compared) and (compared["fills"] == 1).all() and (compared["value"] == 1).all():
            found.append(f"  {parameter} changes nothing: combinations that differ in it alone "
                         f"have the same fills and final value. Does the strategy read it -- and "
                         f"is what it decides reachable?")
    return found


def _where(location) -> str:
    if not location or location == "the strategy":
        return ""
    filename, _, line = location.rpartition(", line ")
    return f"{os.path.basename(filename)}:{line}: "
