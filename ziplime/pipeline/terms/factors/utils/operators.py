from operator import attrgetter
from numbers import Number
from textwrap import dedent


from ziplime.pipeline.errors.bad_binary_operator import BadBinaryOperator
from ziplime.pipeline.terms.term import Term
from ziplime.utils.compat import wraps

from ziplime.pipeline.expression import (
    is_comparison,
    method_name_for_op,
    NumericalExpression,
    NUMEXPR_MATH_FUNCS,
    unary_op_name,
)
from ziplime.pipeline.terms.filters import   NumExprFilter

from ziplime.utils.functional import with_doc, with_name
from ziplime.utils.numpy_utils import (
    bool_dtype,
    coerce_to_dtype,
    float64_dtype,
)


def coerce_numbers_to_my_dtype(f):
    """
    A decorator for methods whose signature is f(self, other) that coerces
    ``other`` to ``self.dtype``.

    This is used to make comparison operations between numbers and `Factor`
    instances work independently of whether the user supplies a float or
    integer literal.

    For example, if I write::

        my_filter = my_factor > 3

    my_factor probably has dtype float64, but 3 is an int, so we want to coerce
    to float64 before doing the comparison.
    """

    @wraps(f)
    def method(self, other):
        if isinstance(other, Number):
            other = coerce_to_dtype(self.dtype, other)
        return f(self, other)

    return method


def binop_return_dtype(op, left, right):
    """
    Compute the expected return dtype for the given binary operator.

    Parameters
    ----------
    op : str
        Operator symbol, (e.g. '+', '-', ...).
    left : numpy.dtype
        Dtype of left hand side.
    right : numpy.dtype
        Dtype of right hand side.

    Returns
    -------
    outdtype : numpy.dtype
        The dtype of the result of `left <op> right`.
    """
    if is_comparison(op):
        if left != right:
            raise TypeError(
                "Don't know how to compute {left} {op} {right}.\n"
                "Comparisons are only supported between Factors of equal "
                "dtypes.".format(left=left, op=op, right=right)
            )
        return bool_dtype

    elif left != float64_dtype or right != float64_dtype:
        raise TypeError(
            "Don't know how to compute {left} {op} {right}.\n"
            "Arithmetic operators are only supported between Factors of "
            "dtype 'float64'.".format(
                left=left.name,
                op=op,
                right=right.name,
            )
        )
    return float64_dtype


BINOP_DOCSTRING_TEMPLATE = """
Construct a :class:`~ziplime.pipeline.{rtype}` computing ``self {op} other``.

Parameters
----------
other : ziplime.pipeline.Factor, float
    Right-hand side of the expression.

Returns
-------
{ret}
"""

BINOP_RETURN_FILTER = """\
filter : ziplime.pipeline.Filter
    Filter computing ``self {op} other`` with the outputs of ``self`` and
    ``other``.
"""

BINOP_RETURN_FACTOR = """\
factor : ziplime.pipeline.Factor
    Factor computing ``self {op} other`` with outputs of ``self`` and
    ``other``.
"""


def binary_operator(op):

    """
    Factory function for making binary operator methods on a Factor subclass.

    Returns a function, "binary_operator" suitable for implementing functions
    like __add__.
    """
    # When combining a Factor with a NumericalExpression, we use this
    # attrgetter instance to defer to the commuted implementation of the
    # NumericalExpression operator.
    commuted_method_getter = attrgetter(method_name_for_op(op, commute=True))

    is_compare = is_comparison(op)

    if is_compare:
        ret_doc = BINOP_RETURN_FILTER.format(op=op)
        rtype = "Filter"
    else:
        ret_doc = BINOP_RETURN_FACTOR.format(op=op)
        rtype = "Factor"

    docstring = BINOP_DOCSTRING_TEMPLATE.format(
        op=op,
        ret=ret_doc,
        rtype=rtype,
    )

    @with_doc(docstring)
    @with_name(method_name_for_op(op))
    @coerce_numbers_to_my_dtype
    def binary_operator(self, other):
        from ziplime.pipeline.terms.factors.num_expr_factor import NumExprFactor

        # This can't be hoisted up a scope because the types returned by
        # binop_return_type aren't defined when the top-level function is
        # invoked in the class body of Factor.
        return_type = NumExprFilter if is_compare else NumExprFactor

        if isinstance(self, NumExprFactor):
            self_expr, other_expr, new_inputs = self.build_binary_op(
                op,
                other,
            )
            return return_type(
                "({left}) {op} ({right})".format(
                    left=self_expr,
                    op=op,
                    right=other_expr,
                ),
                new_inputs,
                dtype=binop_return_dtype(op, self.dtype, other.dtype),
            )
        elif isinstance(other, NumExprFactor):
            # NumericalExpression overrides ops to correctly handle merging of
            # inputs.  Look up and call the appropriate reflected operator with
            # ourself as the input.
            return commuted_method_getter(other)(self)
        elif isinstance(other, Term):
            if self is other:
                return return_type(
                    "x_0 {op} x_0".format(op=op),
                    (self,),
                    dtype=binop_return_dtype(op, self.dtype, other.dtype),
                )
            return return_type(
                "x_0 {op} x_1".format(op=op),
                (self, other),
                dtype=binop_return_dtype(op, self.dtype, other.dtype),
            )
        elif isinstance(other, Number):
            return return_type(
                "x_0 {op} ({constant})".format(op=op, constant=other),
                binds=(self,),
                # .dtype access is safe here because coerce_numbers_to_my_dtype
                # will convert any input numbers to numpy equivalents.
                dtype=binop_return_dtype(op, self.dtype, other.dtype),
            )
        raise BadBinaryOperator(op, self, other)

    return binary_operator


def reflected_binary_operator(op):
    """
    Factory function for making binary operator methods on a Factor.

    Returns a function, "reflected_binary_operator" suitable for implementing
    functions like __radd__.
    """

    assert not is_comparison(op)

    @with_name(method_name_for_op(op, commute=True))
    @coerce_numbers_to_my_dtype
    def reflected_binary_operator(self, other):
        from ziplime.pipeline.terms.factors.num_expr_factor import NumExprFactor

        if isinstance(self, NumericalExpression):
            self_expr, other_expr, new_inputs = self.build_binary_op(op, other)
            return NumExprFactor(
                "({left}) {op} ({right})".format(
                    left=other_expr,
                    right=self_expr,
                    op=op,
                ),
                new_inputs,
                dtype=binop_return_dtype(op, other.dtype, self.dtype),
            )

        # Only have to handle the numeric case because in all other valid cases
        # the corresponding left-binding method will be called.
        elif isinstance(other, Number):
            return NumExprFactor(
                "{constant} {op} x_0".format(op=op, constant=other),
                binds=(self,),
                dtype=binop_return_dtype(op, other.dtype, self.dtype),
            )
        raise BadBinaryOperator(op, other, self)

    return reflected_binary_operator


def unary_operator(op):
    """
    Factory function for making unary operator methods for Factors.
    """

    # Only negate is currently supported.
    valid_ops = {"-"}
    if op not in valid_ops:
        raise ValueError("Invalid unary operator %s." % op)

    @with_doc("Unary Operator: '%s'" % op)
    @with_name(unary_op_name(op))
    def unary_operator(self):
        from ziplime.pipeline.terms.factors.num_expr_factor import NumExprFactor

        if self.dtype != float64_dtype:
            raise TypeError(
                "Can't apply unary operator {op!r} to instance of "
                "{typename!r} with dtype {dtypename!r}.\n"
                "{op!r} is only supported for Factors of dtype "
                "'float64'.".format(
                    op=op,
                    typename=type(self).__name__,
                    dtypename=self.dtype.name,
                )
            )

        # This can't be hoisted up a scope because the types returned by
        # unary_op_return_type aren't defined when the top-level function is
        # invoked.
        if isinstance(self, NumericalExpression):
            return NumExprFactor(
                "{op}({expr})".format(op=op, expr=self._expr),
                self.inputs,
                dtype=float64_dtype,
            )
        else:
            return NumExprFactor(
                "{op}x_0".format(op=op),
                (self,),
                dtype=float64_dtype,
            )

    return unary_operator


def function_application(func):
    """
    Factory function for producing function application methods for Factor
    subclasses.
    """

    if func not in NUMEXPR_MATH_FUNCS:
        raise ValueError("Unsupported mathematical function '%s'" % func)

    docstring = dedent(
        """\
        Construct a Factor that computes ``{}()`` on each output of ``self``.

        Returns
        -------
        factor : ziplime.pipeline.Factor
        """.format(
            func
        )
    )

    @with_doc(docstring)
    @with_name(func)
    def mathfunc(self):
        from ziplime.pipeline.terms.factors.num_expr_factor import NumExprFactor

        if isinstance(self, NumericalExpression):
            return NumExprFactor(
                "{func}({expr})".format(func=func, expr=self._expr),
                self.inputs,
                dtype=float64_dtype,
            )
        else:
            return NumExprFactor(
                "{func}(x_0)".format(func=func),
                (self,),
                dtype=float64_dtype,
            )

    return mathfunc


# Decorators for Factor methods.
# if_not_float64_tell_caller_to_use_isnull = restrict_to_dtype(
#     dtype=float64_dtype,
#     message_template=(
#         "{method_name}() was called on a factor of dtype {received_dtype}.\n"
#         "{method_name}() is only defined for dtype {expected_dtype}."
#         "To filter missing data, use isnull() or notnull()."
#     ),
# )

# float64_only = restrict_to_dtype(
#     dtype=float64_dtype,
#     message_template=(
#         "{method_name}() is only defined on Factors of dtype {expected_dtype},"
#         " but it was called on a Factor of dtype {received_dtype}."
#     ),
# )