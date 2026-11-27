"""Statements for convert."""

import ast
import copy
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.convert.support import DEAD
from ziplime.sweep.convert.support import M
from ziplime.sweep.convert.support import RESULT
from ziplime.sweep.convert.support import RT
from ziplime.sweep.convert.support import SKIP
from ziplime.sweep.convert.expressions import _Expressions
from ziplime.sweep.convert.ast_utils import _carried
from ziplime.sweep.convert.ast_utils import _expression
from ziplime.sweep.convert.ast_utils import _is_sort
from ziplime.sweep.convert.ast_utils import _load
from ziplime.sweep.convert.ast_utils import _template


class _Statements:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.counter = 0
        self.expressions = _Expressions()

    def fresh(self, prefix: str) -> str:
        self.counter += 1
        return f"_gpu_{prefix}{self.counter}"

    def expr(self, node: ast.expr):
        return self.expressions.visit(node)

    def refuse(self, node: ast.AST, reason: str):
        raise NotConvertible(f"{self.filename}, line {node.lineno}: {reason}")

    def block(self, statements: list[ast.stmt]) -> list[ast.stmt]:
        result = []
        for statement in statements:
            for new in self.statement(statement):
                result.append(ast.copy_location(new, statement))
                for child in ast.walk(new):
                    if not hasattr(child, "lineno") and "lineno" in child._attributes:
                        ast.copy_location(child, statement)
        return result or [ast.Pass()]

    def assign(self, target: ast.expr, value: ast.expr) -> list[ast.stmt]:
        if isinstance(target, (ast.Tuple, ast.List)):
            temporary = self.fresh("t")
            result = _template(f"{temporary} = VALUE", VALUE=value)
            for index, element in enumerate(target.elts):
                result += self.assign(element, _expression(f"{temporary}[{index}]"))
            return result
        if isinstance(target, ast.Starred):
            self.refuse(target, "starred assignment is not supported.")
        return _template(f"TARGET = {RT}.sel({M}, VALUE, lambda: OLD)",
                         TARGET=target, VALUE=value, OLD=_load(target))

    def statement(self, node: ast.stmt) -> list[ast.stmt]:
        mask = _template(f"{RT}.mask = {M}")
        if isinstance(node, ast.Expr) and _is_sort(node.value):
            # lst.sort(key=...) on a list of instruments ranks them per combination
            call = node.value
            receiver = call.func.value
            value = ast.Call(func=_expression(f"{RT}.sort_list"),
                             args=[self.expr(_load(receiver))],
                             keywords=[ast.keyword(arg=k.arg, value=self.expr(k.value))
                                       for k in call.keywords])
            target = copy.deepcopy(receiver)
            target.ctx = ast.Store()
            return mask + self.assign(target, value)
        if isinstance(node, ast.Expr):
            return mask + [ast.Expr(self.expr(node.value))]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return self.function(node)
        if isinstance(node, ast.Assign):
            value = self.expr(node.value)
            if len(node.targets) == 1:
                return mask + self.assign(node.targets[0], value)
            temporary = self.fresh("t")
            result = mask + _template(f"{temporary} = VALUE", VALUE=value)
            for target in node.targets:
                result += self.assign(target, _expression(temporary))
            return result
        if isinstance(node, ast.AnnAssign):
            return mask + self.assign(node.target, self.expr(node.value)) if node.value else []
        if isinstance(node, ast.AugAssign):
            value = ast.BinOp(left=_load(node.target), op=node.op, right=self.expr(node.value))
            return mask + self.assign(node.target, value)
        if isinstance(node, ast.Return):
            result = []
            if node.value is not None:
                result = mask + _template(f"{RESULT} = {RT}.returned({M}, VALUE, {RESULT})",
                                          VALUE=self.expr(node.value))
            return result + _template(f"""
                {DEAD} = {RT}.or_({DEAD}, lambda: {M})
                if {DEAD} is True:
                    return {RESULT}
                {M} = False
            """)
        if isinstance(node, ast.Continue):
            return _template(f"""
                {SKIP} = {RT}.or_({SKIP}, lambda: {M})
                {M} = False
            """)
        if isinstance(node, ast.Break):
            return _template(f"""
                if {RT}.plain({M}, {node.lineno}, "break"):
                    break
            """)
        if isinstance(node, ast.If):
            return self.if_(node)
        if isinstance(node, (ast.For, ast.AsyncFor)):
            return self.for_(node)
        if isinstance(node, ast.While):
            return self.while_(node)
        if isinstance(node, ast.Raise):
            return [ast.If(test=_expression(f'{RT}.plain({M}, {node.lineno}, "raise")'),
                           body=[node], orelse=[])]
        if isinstance(node, (ast.Pass, ast.Import, ast.ImportFrom, ast.Global, ast.Nonlocal)):
            return [node]
        if isinstance(node, ast.Delete):
            result = []
            for target in node.targets:
                if not isinstance(target, ast.Subscript):
                    self.refuse(node, "del of anything but a container's item is not supported.")
                result += mask + [ast.Expr(_expression(
                    f"{RT}.delete(CONTAINER, KEY, {node.lineno})",
                    CONTAINER=self.expr(_load(target.value)), KEY=self.expr(_load(target.slice))))]
            return result
        if isinstance(node, ast.Assert):
            return [ast.Assert(test=_expression(f'{RT}.plain(TEST, {node.lineno}, "assert")',
                                                TEST=self.expr(node.test)), msg=node.msg)]
        names = {ast.Try: "try", ast.With: "with", ast.AsyncWith: "async with",
                 ast.FunctionDef: "a nested function", ast.AsyncFunctionDef: "a nested function",
                 ast.ClassDef: "a class"}
        self.refuse(node, f"{names.get(type(node), type(node).__name__)} is not supported in a "
                          f"function the sweep runs for every combination.")

    def function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.stmt]:
        """A nested function: converted like the one around it, and run under the mask of the
        statement that calls it."""
        if node.decorator_list:
            self.refuse(node, "a decorated nested function is not supported.")
        if any(isinstance(child, (ast.Yield, ast.YieldFrom)) for child in ast.walk(node)):
            self.refuse(node, "a generator is not supported in a function the sweep runs for every "
                              "combination.")
        inner = self.fresh("f")
        body = _template(f"""
            {M} = {RT}.mask
            {DEAD} = False
            {SKIP} = False
            {RESULT} = None
        """) + self.block(node.body) + _template(f"return {RESULT}")
        arguments = copy.deepcopy(node.args)
        for argument in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs,
                         arguments.vararg, arguments.kwarg):
            if argument is not None:
                argument.annotation = None
        arguments.defaults = [self.expr(d) for d in arguments.defaults]
        arguments.kw_defaults = [self.expr(d) if d is not None else None
                                 for d in arguments.kw_defaults]
        definition = ast.FunctionDef(name=inner, args=arguments, body=body, decorator_list=[],
                                     returns=None, type_params=[])
        return [definition] + _template(f"{node.name} = {RT}.nested({inner})")

    def if_(self, node: ast.If) -> list[ast.stmt]:
        condition, saved = self.fresh("c"), self.fresh("s")
        result = _template(f"""
            {condition} = {RT}.truth(TEST)
            {saved} = {M}
            {M} = {RT}.and_({saved}, lambda: {condition})
        """, TEST=self.expr(node.test))
        result.append(ast.If(test=_expression(f"{M} is not False"), body=self.block(node.body),
                             orelse=[]))
        if node.orelse:
            result += _template(f"""
                {M} = {RT}.and_({RT}.and_({saved}, lambda: {RT}.not_({condition})),
                                lambda: {RT}.alive({DEAD}, {SKIP}))
            """)
            result.append(ast.If(test=_expression(f"{M} is not False"),
                                 body=self.block(node.orelse), orelse=[]))
        result += _template(f"{M} = {RT}.and_({saved}, lambda: {RT}.alive({DEAD}, {SKIP}))")
        return result

    def for_(self, node: ast.For) -> list[ast.stmt]:
        if node.orelse:
            self.refuse(node, "for ... else is not supported.")
        skip, path = self.fresh("k"), self.fresh("p")
        carried = _carried(node)
        result = _template(f"""
            {skip} = {SKIP}
            {path} = {M}
        """)
        items = self.fresh("i")
        result += _template(f"{items} = {RT}.loop(ITER, {carried!r}, {node.lineno})",
                            ITER=self.expr(node.iter))
        body = _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
            {M} = {RT}.and_({M}, lambda: {RT}.loop_mask({items}))
        """) + [ast.If(test=_expression(f"{M} is not False"), body=self.block(node.body), orelse=[])]
        result.append(ast.For(target=node.target, iter=ast.Name(items, ast.Load()), body=body,
                              orelse=[]))
        result += _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
        """)
        return result

    def while_(self, node: ast.While) -> list[ast.stmt]:
        if node.orelse:
            self.refuse(node, "while ... else is not supported.")
        skip, path = self.fresh("k"), self.fresh("p")
        result = _template(f"""
            {skip} = {SKIP}
            {path} = {M}
        """)
        test = _expression(f'{RT}.plain({RT}.and_({M}, lambda: TEST), {node.lineno}, "while")',
                           TEST=self.expr(node.test))
        body = self.block(node.body) + _template(f"""
            {SKIP} = {skip}
            {M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))
        """)
        result.append(ast.While(test=test, body=body, orelse=[]))
        result += _template(f"{M} = {RT}.and_({path}, lambda: {RT}.alive({DEAD}, {SKIP}))")
        return result
