from ziplime.pipeline.expression import NumericalExpression
from ziplime.pipeline.terms.filters.filter import Filter
from ziplime.utils.numpy_utils import bool_dtype


class NumExprFilter(NumericalExpression, Filter):
    """
    A Filter computed from a numexpr expression.
    """

    @classmethod
    def create(cls, expr, binds):
        """
        Helper for creating new NumExprFactors.

        This is just a wrapper around NumericalExpression.__new__ that always
        forwards `bool` as the dtype, since Filters can only be of boolean
        dtype.
        """
        return cls(expr=expr, binds=binds, dtype=bool_dtype)

    def _compute(self, arrays, dates, assets, mask):
        """
        Compute our result with numexpr, then re-apply `mask`.
        """
        return (
            super(NumExprFilter, self)._compute(
                arrays,
                dates,
                assets,
                mask,
            )
            & mask
        )