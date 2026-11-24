"""Compiler for convert."""

import ast
import copy
import types
from collections.abc import Callable
from ziplime.sweep.convert.support import DEAD
from ziplime.sweep.convert.support import M
from ziplime.sweep.convert.support import RESULT
from ziplime.sweep.convert.support import RT
from ziplime.sweep.convert.support import SKIP
from ziplime.sweep.convert.statements import _Statements
from ziplime.sweep.convert.source_utils import _source
from ziplime.sweep.convert.ast_utils import _template


def _compile(function: Callable) -> types.FunctionType:
    """The converted function; it takes the Runtime as an extra last argument."""
    definition, filename, first_line = _source(function)
    statements = _Statements(filename)
    body = _template(f"""
        {M} = {RT}.mask
        {DEAD} = False
        {SKIP} = False
        {RESULT} = None
    """) + statements.block(definition.body) + _template(f"return {RESULT}")
    arguments = copy.deepcopy(definition.args)
    # Annotations name types the module may only import for type checking
    for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs,
                     arguments.vararg, arguments.kwarg):
        if argument is not None:
            argument.annotation = None
    arguments.args.append(ast.arg(arg=RT))
    new = ast.FunctionDef(name=definition.name, args=arguments, body=body, decorator_list=[],
                          returns=None, type_params=[])
    module = ast.Module(body=[ast.copy_location(new, definition)], type_ignores=[])
    ast.increment_lineno(module, first_line - 1)
    ast.fix_missing_locations(module)
    namespace = {}
    exec(compile(module, filename, "exec"), namespace)
    return namespace[definition.name]
