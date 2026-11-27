"""Symbol utils for initialize."""

import ast
import inspect
import textwrap
from collections.abc import Callable


def symbols_in(function: Callable) -> list[tuple[str, str | None]]:
    """The tickers a hook passes to context.symbol / context.symbols: string constants, and the
    items of a list of them the call's argument loops over
    (``{s: await context.symbol(s) for s in ["SOXL", "TQQQ"]}``)."""
    try:
        definition = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
    except (OSError, TypeError, SyntaxError, IndexError):
        return []
    if not isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef)) or not definition.args.args:
        return []
    context = definition.args.args[0].arg
    lists = {}
    for node in ast.walk(definition):
        if isinstance(node, ast.Assign) and isinstance(node.value, (ast.List, ast.Tuple)) and \
                all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in node.value.elts):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    lists[target.id] = [e.value for e in node.value.elts]

    def items(node: ast.AST):
        if isinstance(node, (ast.List, ast.Tuple)):
            return [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        if isinstance(node, ast.Name):
            return lists.get(node.id, [])
        return []

    loops = {}  # loop variable -> the tickers it takes
    for node in ast.walk(definition):
        if isinstance(node, (ast.For, ast.comprehension)) and isinstance(node.target, ast.Name):
            loops.setdefault(node.target.id, []).extend(items(node.iter))
    found = []
    for node in ast.walk(definition):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("symbol", "symbols") and isinstance(node.func.value, ast.Name)
                and node.func.value.id == context):
            continue
        mic = next((k.value.value for k in node.keywords
                    if k.arg == "mic" and isinstance(k.value, ast.Constant)), None)
        for arg in (node.args if node.func.attr == "symbols" else node.args[:1]):
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                tickers = [arg.value]
            elif isinstance(arg, ast.Name):
                tickers = loops.get(arg.id, [])
            else:
                tickers = []
            found += [(ticker, mic) for ticker in tickers if (ticker, mic) not in found]
    return found
