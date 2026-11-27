"""Factories for filter operators."""

from operator import attrgetter
from ziplime.pipeline.errors.bad_binary_operator import BadBinaryOperator
from ziplime.pipeline.expression_utils import method_name_for_op
from ziplime.pipeline.expression import NumericalExpression
from ziplime.pipeline.terms.term import Term
from ziplime.utils.numpy_utils import bool_dtype


def binary_operator(op: str):
    """
    Factory function for making binary operator methods on a Filter subclass.

    Returns a function "binary_operator" suitable for implementing functions
    like __and__ or __or__.
    """
    # When combining a Filter with a NumericalExpression, we use this
    # attrgetter instance to defer to the commuted interpretation of the
    # NumericalExpression operator.
    commuted_method_getter = attrgetter(method_name_for_op(op, commute=True))

    def binary_operator(self, other: Term | int):
        from ziplime.pipeline.terms.filters.num_expr_filter import NumExprFilter
        if isinstance(self, NumericalExpression):
            self_expr, other_expr, new_inputs = self.build_binary_op(
                op,
                other,
            )
            return NumExprFilter.create(
                "({left}) {op} ({right})".format(
                    left=self_expr,
                    op=op,
                    right=other_expr,
                ),
                new_inputs,
            )
        elif isinstance(other, NumericalExpression):
            # NumericalExpression overrides numerical ops to correctly handle
            # merging of inputs.  Look up and call the appropriate
            # right-binding operator with ourself as the input.
            return commuted_method_getter(other)(self)
        elif isinstance(other, Term):
            if other.dtype != bool_dtype:
                raise BadBinaryOperator(op, self, other)
            if self is other:
                return NumExprFilter.create(
                    "x_0 {op} x_0".format(op=op),
                    (self,),
                )
            return NumExprFilter.create(
                "x_0 {op} x_1".format(op=op),
                (self, other),
            )
        elif isinstance(other, int):  # Note that this is true for bool as well
            return NumExprFilter.create(
                "x_0 {op} {constant}".format(op=op, constant=int(other)),
                binds=(self,),
            )
        raise BadBinaryOperator(op, self, other)

    binary_operator.__doc__ = "Binary Operator: '%s'" % op
    return binary_operator


def unary_operator(op: str):
    """
    Factory function for making unary operator methods for Filters.
    """
    valid_ops = {"~"}
    if op not in valid_ops:
        raise ValueError("Invalid unary operator %s." % op)

    def unary_operator(self):
        from ziplime.pipeline.terms.filters.num_expr_filter import NumExprFilter
        # This can't be hoisted up a scope because the types returned by
        # unary_op_return_type aren't defined when the top-level function is
        # invoked.
        if isinstance(self, NumericalExpression):
            return NumExprFilter.create(
                "{op}({expr})".format(op=op, expr=self._expr),
                self.inputs,
            )
        else:
            return NumExprFilter.create("{op}x_0".format(op=op), (self,))

    unary_operator.__doc__ = "Unary Operator: '%s'" % op
    return unary_operator
