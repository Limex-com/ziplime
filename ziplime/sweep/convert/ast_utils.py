"""Ast utils for convert."""

import ast
import copy
import textwrap


def _template(code: str, **nodes: ast.AST) -> list[ast.stmt]:
    tree = ast.parse(textwrap.dedent(code))
    # The template's own lines (1, 2...) would point at the top of the strategy's function: they
    # take the line of the statement they are generated for instead (block, fix_missing_locations)
    for node in ast.walk(tree):
        for attribute in ("lineno", "col_offset", "end_lineno", "end_col_offset"):
            if hasattr(node, attribute):
                delattr(node, attribute)

    class Fill(ast.NodeTransformer):
        def visit_Name(self, node: ast.Name):
            if node.id in nodes:
                replacement = copy.deepcopy(nodes[node.id])
                if hasattr(replacement, "ctx"):
                    replacement.ctx = node.ctx
                return replacement
            return node

    return Fill().visit(tree).body


def _expression(code: str, **nodes: ast.AST) -> ast.expr:
    return _template(code, **nodes)[0].value


def _load(node: ast.expr) -> ast.expr:
    node = copy.deepcopy(node)
    for child in ast.walk(node):
        if hasattr(child, "ctx"):
            child.ctx = ast.Load()
    return node


def _is_sort(node: ast.AST) -> bool:
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
