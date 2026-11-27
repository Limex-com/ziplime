"""Utils for code_rules."""

from __future__ import annotations
import ast
from ziplime.mcp.code_rules.walker import _Walker
from ziplime.mcp.code_rules.hook_utils import _hook_problems
from ziplime.mcp.code_rules.diagnostics import _problem


def check(code: str) -> dict:
    """Everything wrong with this strategy that a syntax tree can show.

    Returns the problems, whether any of them blocks, and the rules text, so a
    caller that finds a problem also has what it needs to fix it.
    """
    from ziplime.mcp import docs

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
