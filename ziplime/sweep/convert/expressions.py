"""Expressions for convert."""

import ast
import copy
from ziplime.sweep.convert.support import RT
from ziplime.sweep.convert.ast_utils import _expression


class _Expressions(ast.NodeTransformer):
    """and/or/not/if-else/chained comparisons to elementwise; await dropped."""

    def visit_Await(self, node: ast.Await):
        return self.visit(node.value)

    def visit_BoolOp(self, node: ast.BoolOp):
        self.generic_visit(node)
        method = "and_" if isinstance(node.op, ast.And) else "or_"
        result = node.values[0]
        for value in node.values[1:]:
            result = _expression(f"{RT}.{method}(LEFT, lambda: RIGHT)", LEFT=result, RIGHT=value)
        return result

    def visit_UnaryOp(self, node: ast.UnaryOp):
        self.generic_visit(node)
        if isinstance(node.op, ast.Not):
            return _expression(f"{RT}.not_(VALUE)", VALUE=node.operand)
        return node

    def visit_IfExp(self, node: ast.IfExp):
        self.generic_visit(node)
        return _expression(f"{RT}.where(TEST, YES, NO)", TEST=node.test, YES=node.body, NO=node.orelse)

    def visit_Compare(self, node: ast.Compare):
        self.generic_visit(node)
        if len(node.ops) == 1:
            op = node.ops[0]
            if isinstance(op, (ast.In, ast.NotIn)):
                test = _expression(f"{RT}.contains(ITEM, CONTAINER)", ITEM=node.left,
                                   CONTAINER=node.comparators[0])
                return test if isinstance(op, ast.In) else _expression(f"{RT}.not_(TEST)", TEST=test)
            if isinstance(op, (ast.Is, ast.IsNot)) and isinstance(node.comparators[0], ast.Constant) \
                    and node.comparators[0].value is None:
                test = _expression(f"{RT}.is_none(VALUE)", VALUE=node.left)
                return test if isinstance(op, ast.Is) else _expression(f"{RT}.not_(TEST)", TEST=test)
            return node
        parts = [ast.Compare(left=left, ops=[op], comparators=[right])
                 for left, op, right in zip([node.left, *node.comparators], node.ops, node.comparators)]
        result = parts[0]
        for part in parts[1:]:
            result = _expression(f"{RT}.and_(LEFT, lambda: RIGHT)", LEFT=result, RIGHT=part)
        return result

    def visit_Lambda(self, node: ast.Lambda):
        return node  # a lambda's body is not a statement of the strategy

    def visit_Dict(self, node: ast.Dict):
        self.generic_visit(node)
        return _expression(f"{RT}.dict(VALUE)", VALUE=node)

    def visit_DictComp(self, node: ast.DictComp):
        pair = ast.GeneratorExp(elt=ast.Tuple(elts=[node.key, node.value], ctx=ast.Load()),
                                generators=node.generators)
        converted = self._comprehension(pair, kind="dict")
        return _expression(f"{RT}.dict(VALUE)", VALUE=node) if converted is pair else converted

    def visit_GeneratorExp(self, node: ast.GeneratorExp):
        return self._comprehension(node)

    def visit_ListComp(self, node: ast.ListComp):
        return self._comprehension(node)

    def _comprehension(self, node: ast.GeneratorExp | ast.ListComp, kind: str | None = None):
        """``(f(x) for x in xs if c(x))``: over instruments, one array with a mask -- a cross
        section that sum/any/all/max/min/len reduce per combination; otherwise as Python."""
        if len(node.generators) != 1 or node.generators[0].is_async:
            return node
        generator = node.generators[0]
        names = ([generator.target.id] if isinstance(generator.target, ast.Name) else
                 [element.id for element in getattr(generator.target, "elts", [])
                  if isinstance(element, ast.Name)])
        if not names or (not isinstance(generator.target, ast.Name)
                         and len(names) != len(generator.target.elts)):
            return node
        arguments = ast.arguments(posonlyargs=[], args=[ast.arg(arg=n) for n in names],
                                  kwonlyargs=[], kw_defaults=[], defaults=[])
        value = ast.Lambda(args=arguments, body=self.visit(node.elt))
        condition = None
        for test in generator.ifs:
            test = self.visit(test)
            condition = test if condition is None else _expression(
                f"{RT}.and_(LEFT, lambda: RIGHT)", LEFT=condition, RIGHT=test)
        condition = ast.Lambda(args=copy.deepcopy(arguments),
                               body=condition if condition is not None else ast.Constant(True))
        kind = kind or ("list" if isinstance(node, ast.ListComp) else "generator")
        return _expression(f"{RT}.comprehension(ITER, VALUE, CONDITION, {len(names)}, {kind!r})",
                           ITER=self.visit(generator.iter), VALUE=value, CONDITION=condition)
