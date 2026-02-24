from operator import attrgetter

from ziplime.errors import UnsupportedDataType
from ziplime.pipeline.dtypes import FILTER_DTYPES
from ziplime.pipeline.errors.bad_binary_operator import BadBinaryOperator
from ziplime.pipeline.expression import (
    FILTER_BINOPS,
    method_name_for_op,
    NumericalExpression,
)
from ziplime.pipeline.mixins import IfElseMixin, RestrictedDTypeMixin
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.terms.computable_term import ComputableTerm

from ziplime.utils.numpy_utils import same, bool_dtype


def binary_operator(op):
    """
    Factory function for making binary operator methods on a Filter subclass.

    Returns a function "binary_operator" suitable for implementing functions
    like __and__ or __or__.
    """
    # When combining a Filter with a NumericalExpression, we use this
    # attrgetter instance to defer to the commuted interpretation of the
    # NumericalExpression operator.
    commuted_method_getter = attrgetter(method_name_for_op(op, commute=True))

    def binary_operator(self, other):
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


def unary_operator(op):
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


class Filter(RestrictedDTypeMixin, ComputableTerm):
    """
    Pipeline expression computing a boolean output.

    Filters are most commonly useful for describing sets of assets to include
    or exclude for some particular purpose. Many Pipeline API functions accept
    a ``mask`` argument, which can be supplied a Filter indicating that only
    values passing the Filter should be considered when performing the
    requested computation. For example, :meth:`ziplime.pipeline.Factor.top`
    accepts a mask indicating that ranks should be computed only on assets that
    passed the specified Filter.

    The most common way to construct a Filter is via one of the comparison
    operators (``<``, ``<=``, ``!=``, ``eq``, ``>``, ``>=``) of
    :class:`~ziplime.pipeline.Factor`. For example, a natural way to construct
    a Filter for stocks with a 10-day VWAP less than $20.0 is to first
    construct a Factor computing 10-day VWAP and compare it to the scalar value
    20.0::

        >>> from ziplime.pipeline.factors import VWAP
        >>> vwap_10 = VWAP(window_length=10)
        >>> vwaps_under_20 = (vwap_10 <= 20)

    Filters can also be constructed via comparisons between two Factors.  For
    example, to construct a Filter producing True for asset/date pairs where
    the asset's 10-day VWAP was greater than it's 30-day VWAP::

        >>> short_vwap = VWAP(window_length=10)
        >>> long_vwap = VWAP(window_length=30)
        >>> higher_short_vwap = (short_vwap > long_vwap)

    Filters can be combined via the ``&`` (and) and ``|`` (or) operators.

    ``&``-ing together two filters produces a new Filter that produces True if
    **both** of the inputs produced True.

    ``|``-ing together two filters produces a new Filter that produces True if
    **either** of its inputs produced True.

    The ``~`` operator can be used to invert a Filter, swapping all True values
    with Falses and vice-versa.

    Filters may be set as the ``screen`` attribute of a Pipeline, indicating
    asset/date pairs for which the filter produces False should be excluded
    from the Pipeline's output.  This is useful both for reducing noise in the
    output of a Pipeline and for reducing memory consumption of Pipeline
    results.
    """

    # Filters are window-safe by default, since a yes/no decision means the
    # same thing from all temporal perspectives.
    window_safe = True

    # Used by RestrictedDTypeMixin
    ALLOWED_DTYPES = FILTER_DTYPES
    dtype = bool_dtype

    clsdict = locals()
    clsdict.update(
        {method_name_for_op(op): binary_operator(op) for op in FILTER_BINOPS}
    )
    clsdict.update(
        {
            method_name_for_op(op, commute=True): binary_operator(op)
            for op in FILTER_BINOPS
        }
    )

    __invert__ = unary_operator("~")

    def _validate(self):
        # Run superclass validation first so that we handle `dtype not passed`
        # before this.
        retval = super(Filter, self)._validate()
        if self.dtype != bool_dtype:
            raise UnsupportedDataType(typename=type(self).__name__, dtype=self.dtype)
        return retval

    @classmethod
    def _principal_computable_term_type(cls):
        return Filter

    def if_else(self, if_true: ComputableTerm, if_false: ComputableTerm):
        """
        Create a term that selects values from one of two choices.

        Parameters
        ----------
        if_true : ziplime.pipeline.term.ComputableTerm
            Expression whose values should be used at locations where this
            filter outputs True.
        if_false : ziplime.pipeline.term.ComputableTerm
            Expression whose values should be used at locations where this
            filter outputs False.

        Returns
        -------
        merged : ziplime.pipeline.term.ComputableTerm
           A term that computes by taking values from either ``if_true`` or
           ``if_false``, depending on the values produced by ``self``.

           The returned term draws from``if_true`` at locations where ``self``
           produces True, and it draws from ``if_false`` at locations where
           ``self`` produces False.

        Example
        -------

        Let ``f`` be a Factor that produces the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0    2.0    3.0    4.0
            2017-03-14    5.0    6.0    7.0    8.0

        Let ``g`` be another Factor that produces the following output::

                         AAPL   MSFT    MCD     BK
            2017-03-13   10.0   20.0   30.0   40.0
            2017-03-14   50.0   60.0   70.0   80.0

        Finally, let ``condition`` be a Filter that produces the following
        output::

                         AAPL   MSFT    MCD     BK
            2017-03-13   True  False   True  False
            2017-03-14   True   True  False  False

        Then, the expression ``condition.if_else(f, g)`` produces the following
        output::

                         AAPL   MSFT    MCD     BK
            2017-03-13    1.0   20.0    3.0   40.0
            2017-03-14    5.0    6.0   70.0   80.0

        See Also
        --------
        numpy.where
        Factor.fillna
        """
        true_type = if_true._principal_computable_term_type()
        false_type = if_false._principal_computable_term_type()

        if true_type is not false_type:
            raise TypeError(
                "Mismatched types in if_else(): if_true={}, but if_false={}".format(
                    true_type.__name__, false_type.__name__
                )
            )

        if if_true.dtype != if_false.dtype:
            raise TypeError(
                "Mismatched dtypes in if_else(): "
                "if_true.dtype = {}, if_false.dtype = {}".format(
                    if_true.dtype, if_false.dtype
                )
            )

        if if_true.outputs != if_false.outputs:
            raise ValueError(
                "Mismatched outputs in if_else(): "
                "if_true.outputs = {}, if_false.outputs = {}".format(
                    if_true.outputs, if_false.outputs
                ),
            )

        if not same(if_true.missing_value, if_false.missing_value):
            raise ValueError(
                "Mismatched missing values in if_else(): "
                "if_true.missing_value = {!r}, if_false.missing_value = {!r}".format(
                    if_true.missing_value, if_false.missing_value
                )
            )

        return_type = type(if_true)._with_mixin(IfElseMixin)

        return return_type(
            condition=self,
            if_true=if_true,
            if_false=if_false,
        )
