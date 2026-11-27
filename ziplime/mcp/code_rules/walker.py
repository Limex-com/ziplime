"""Walker for code_rules."""

from __future__ import annotations
import ast
from ziplime.mcp.code_rules.support import AWAITED_CALLS
from ziplime.mcp.code_rules.support import PANDAS_ACCESSORS
from ziplime.mcp.code_rules.diagnostics import _problem


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
