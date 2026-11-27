from textwrap import dedent

from ziplime.pipeline.terms.filters import Filter
from ziplime.pipeline.terms.factors.utils.summary_operations import summary_funcs


CORRELATION_METHOD_NOTE = dedent(
    """\
    This method can only be called on expressions which are deemed safe for use
    as inputs to windowed :class:`~ziplime.pipeline.Factor` objects. Examples
    of such expressions include This includes
    :class:`~ziplime.pipeline.data.BoundColumn`
    :class:`~ziplime.pipeline.factors.Returns` and any factors created from
    :meth:`~ziplime.pipeline.Factor.rank` or
    :meth:`~ziplime.pipeline.Factor.zscore`.
    """
)


def summary_method(name: str):

    func = getattr(summary_funcs, name)

    # @float64_only
    def f(self, mask: Filter | None = None):
        """Create a 1-dimensional factor computing the {} of self, each day.

        Parameters
        ----------
        mask : ziplime.pipeline.Filter, optional
           A Filter representing assets to consider when computing results.
           If supplied, we ignore asset/date pairs where ``mask`` produces
           ``False``.

        Returns
        -------
        result : ziplime.pipeline.Factor
        """
        from ziplime.pipeline.terms.factors.daily_summary import DailySummary

        return DailySummary(
            func,
            self,
            mask=mask,
            dtype=self.dtype,
        )

    f.__name__ = func.__name__
    f.__doc__ = f.__doc__.format(f.__name__)

    return f
