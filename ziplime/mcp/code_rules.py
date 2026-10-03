"""An AST pass over a strategy, run before a backtest is spent on it.

The engine validates by executing, which means a broken hook is discovered
after the data is loaded and the clock has started. This is a syntax tree walk
that takes milliseconds and catches the mistakes that cost a run.

Two severities, and the split is the whole design:

- **blocking** — the engine is certain to refuse or the strategy provably
  cannot trade. Syntax errors, a hook that is not `async`, a hook with the
  wrong parameters.
- **advisory** — matched on *names*, so it can be wrong. A local variable
  called `order`, a `.loc` on something that is not a frame. **An advisory
  never blocks a run**: a false positive that stops a correct strategy is
  worse than the mistake it was looking for.
"""
from __future__ import annotations

import ast

#: Hooks the engine looks up by name, and how many positional parameters each
#: is called with. Anything else at module level is the author's own business.
HOOKS = {
    "initialize": 1,
    "handle_data": 2,
    "before_trading_start": 2,
    "analyze": 2,
}
REQUIRED_HOOKS = ("initialize", "handle_data")

#: Coroutine methods on `context` and `data`. Calling one without `await`
#: returns a coroutine, does nothing, and raises nothing.
AWAITED_CALLS = frozenset({
    "symbol", "history", "current",
    "order", "order_value", "order_percent",
    "order_target", "order_target_value", "order_target_percent",
    "get_asset_positions", "get_portfolio", "get_account",
})

#: pandas accessors on what is a Polars frame here.
PANDAS_ACCESSORS = frozenset({"iloc", "loc", "at", "iat"})


def _problem(severity: str, rule: str, message: str, line: int | None = None) -> dict:
    entry = {"severity": severity, "rule": rule, "message": message}
    if line is not None:
        entry["line"] = line
    return entry


def _hook_problems(tree: ast.Module) -> list[dict]:
    found: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in HOOKS:
            found[node.name] = node

    problems: list[dict] = []
    for name in REQUIRED_HOOKS:
        if name not in found:
            problems.append(_problem(
                "blocking" if name == "handle_data" else "advisory",
                "missing_hook",
                f"No `{name}` at module level. "
                + ("Without it the strategy never places an order."
                   if name == "handle_data"
                   else "The engine substitutes a no-op, so this is legal but "
                        "usually means setup code went missing."),
            ))

    for name, node in found.items():
        if isinstance(node, ast.FunctionDef):
            problems.append(_problem(
                "blocking", "hook_not_async",
                f"`{name}` must be `async def` — the engine awaits it, and a "
                f"plain `def` returns None to an `await`.",
                node.lineno,
            ))
        expected = HOOKS[name]
        actual = len(node.args.args)
        if actual != expected:
            problems.append(_problem(
                "blocking", "hook_signature",
                f"`{name}` takes {expected} argument(s), not {actual}. "
                f"The engine calls it positionally.",
                node.lineno,
            ))
    return problems


class _Walker(ast.NodeVisitor):
    """One pass collecting the advisory findings."""

    def __init__(self) -> None:
        self.problems: list[dict] = []
        self._awaited: set[int] = set()

    def visit_Await(self, node: ast.Await) -> None:
        self._awaited.add(id(node.value))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in AWAITED_CALLS:
            if id(node) not in self._awaited:
                self.problems.append(_problem(
                    "advisory", "missing_await",
                    f"`{func.attr}(...)` looks like an engine call and is not "
                    "awaited. Unawaited, it does nothing and raises nothing.",
                    node.lineno,
                ))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in PANDAS_ACCESSORS:
            self.problems.append(_problem(
                "advisory", "pandas_accessor",
                f"`.{node.attr}` is a pandas accessor. Market data here is a "
                "Polars frame — index it as `frame[\"close\"][-1]`.",
                node.lineno,
            ))
        self.generic_visit(node)


def check(code: str) -> dict:
    """Everything wrong with this strategy that a syntax tree can show.

    Returns the problems, whether any of them blocks, and the rules text, so a
    caller that finds a problem also has what it needs to fix it.
    """
    from . import docs

    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return {
            "ok": False,
            "blocking": True,
            "problems": [_problem(
                "blocking", "syntax_error",
                f"{exc.msg} (line {exc.lineno}, column {exc.offset})", exc.lineno,
            )],
            "engine_rules": docs.STRATEGY_RULES,
        }

    walker = _Walker()
    walker.visit(tree)
    problems = _hook_problems(tree) + walker.problems
    problems.sort(key=lambda item: (item["severity"] != "blocking", item.get("line") or 0))
    blocking = any(item["severity"] == "blocking" for item in problems)

    result = {
        "ok": not problems,
        "blocking": blocking,
        "problems": problems,
        "advisories_never_block": (
            "Advisory findings are matched on names and can be wrong. They are "
            "reported, never enforced — a false positive must not stop a run."
        ),
    }
    if problems:
        result["engine_rules"] = docs.STRATEGY_RULES
    return result
