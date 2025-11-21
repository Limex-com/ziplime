from numpy import (
    float64,
    nan,
    nanpercentile,
)

from ziplime.errors import (
    BadPercentileBounds,
)
from ziplime.pipeline.mixins import (
    SingleInputMixin,
)
from ziplime.pipeline.terms.filters import Filter


class PercentileFilter(SingleInputMixin, Filter):
    """
    A Filter representing assets falling between percentile bounds of a Factor.

    Parameters
    ----------
    factor : ziplime.pipeline.factor.Factor
        The factor over which to compute percentile bounds.
    min_percentile : float [0.0, 1.0]
        The minimum percentile rank of an asset that will pass the filter.
    max_percentile : float [0.0, 1.0]
        The maxiumum percentile rank of an asset that will pass the filter.
    """

    window_length = 0

    def __new__(cls, factor, min_percentile, max_percentile, mask):
        return super(PercentileFilter, cls).__new__(
            cls,
            inputs=(factor,),
            mask=mask,
            min_percentile=min_percentile,
            max_percentile=max_percentile,
        )

    def _init(self, min_percentile, max_percentile, *args, **kwargs):
        self._min_percentile = min_percentile
        self._max_percentile = max_percentile
        return super(PercentileFilter, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, min_percentile, max_percentile, *args, **kwargs):
        return (
            super(PercentileFilter, cls)._static_identity(*args, **kwargs),
            min_percentile,
            max_percentile,
        )

    def _validate(self):
        """
        Ensure that our percentile bounds are well-formed.
        """
        if not 0.0 <= self._min_percentile < self._max_percentile <= 100.0:
            raise BadPercentileBounds(
                min_percentile=self._min_percentile,
                max_percentile=self._max_percentile,
                upper_bound=100.0,
            )
        return super(PercentileFilter, self)._validate()

    def _compute(self, arrays, dates, assets, mask):
        """
        For each row in the input, compute a mask of all values falling between
        the given percentiles.
        """
        # TODO: Review whether there's a better way of handling small numbers
        # of columns.
        data = arrays[0].copy().astype(float64)
        data[~mask] = nan

        # FIXME: np.nanpercentile **should** support computing multiple bounds
        # at once, but there's a bug in the logic for multiple bounds in numpy
        # 1.9.2.  It will be fixed in 1.10.
        # c.f. https://github.com/numpy/numpy/pull/5981
        lower_bounds = nanpercentile(
            data,
            self._min_percentile,
            axis=1,
            keepdims=True,
        )
        upper_bounds = nanpercentile(
            data,
            self._max_percentile,
            axis=1,
            keepdims=True,
        )
        return (lower_bounds <= data) & (data <= upper_bounds)

    def graph_repr(self):
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "{}:\\l  min: {}, max: {}\\l".format(
            type(self).__name__,
            self._min_percentile,
            self._max_percentile,
        )