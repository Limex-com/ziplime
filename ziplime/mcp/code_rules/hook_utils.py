"""Hook utils for code_rules."""

from __future__ import annotations
import ast
from ziplime.mcp.code_rules.support import HOOKS
from ziplime.mcp.code_rules.support import REQUIRED_HOOKS
from ziplime.mcp.code_rules.diagnostics import _problem


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
