"""Source utils for convert."""

import ast
import inspect
import textwrap
from collections.abc import Callable
from collections.abc import Iterable
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.support import MUTATING


def _source(function: Callable) -> tuple[ast.FunctionDef | ast.AsyncFunctionDef, str, int]:
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError):
        raise NotConvertible(f"The source of {function.__name__} is not available.") from None
    filename = inspect.getsourcefile(function) or f"<{function.__name__}>"
    first_line = inspect.getsourcelines(function)[1]
    tree = ast.parse(source)
    definition = tree.body[0]
    if not isinstance(definition, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise NotConvertible(f"{function.__name__} is not a function.")
    return definition, filename, first_line


def _text(function: Callable) -> str:
    try:
        return inspect.getsource(function)
    except (OSError, TypeError):
        return ""


def context_writes(functions: Iterable[Callable]) -> tuple[set[str], set[str], list[str]]:
    """``context.X`` attributes the functions assign, those they change in place
    (``context.X[k] = v``, ``del``, ``.pop()``), and reasons the step cannot be compiled.

    A compiled step is traced once, so Python state that changes from bar to bar has to travel as
    arrays. Numbers assigned to ``context.X`` can, and so can a per-instrument dict; other
    containers changed in place and values that vary with the date cannot.
    """
    written, in_place, reasons = set(), set(), []

    def owner(node: ast.AST):
        """``context.X[...]`` / ``context.X.pop`` -> "X"."""
        while isinstance(node, (ast.Subscript, ast.Attribute)):
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id == context):
                return node.attr
            node = node.value
        return None

    for function in functions:
        try:
            definition, filename, first_line = _source(function)
        except NotConvertible:
            continue
        context = definition.args.args[0].arg if definition.args.args else "context"
        for node in ast.walk(definition):
            line = first_line + getattr(node, "lineno", 1) - 1
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
                targets = [node.target]
            elif isinstance(node, ast.Delete):
                targets = node.targets
            for target in targets:
                if (isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name)
                        and target.value.id == context):
                    written.add(target.attr)
                elif isinstance(target, ast.Subscript) and owner(target.value):
                    in_place.add(owner(target.value))
                elif isinstance(target, ast.Subscript) and context in ast.unparse(target.value):
                    reasons.append(f"line {line}: {ast.unparse(target)} is changed in place")
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in MUTATING and owner(node.func.value)):
                in_place.add(owner(node.func.value))
            if isinstance(node, ast.Attribute) and node.attr in (
                "get_datetime", "current_dt", "current_session",
            ):
                reasons.append(f"line {line}: reads the date")
    return written, in_place, reasons
