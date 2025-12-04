from numpy import broadcast_arrays
from scipy.stats import (
    spearmanr,
)

from ziplime.pipeline.terms.factors.statistical.rolling_correlation import RollingCorrelation


class RollingSpearman(RollingCorrelation):
    """
    A Factor that computes spearman rank correlation coefficients between the
    columns of a given Factor and either the columns of another
    Factor/BoundColumn or a slice/single column of data.

    Parameters
    ----------
    base_factor : ziplime.pipeline.Factor
        The factor for which to compute correlations of each of its columns
        with `target`.
    target : ziplime.pipeline.Term with a numeric dtype
        The term with which to compute correlations against each column of data
        produced by `base_factor`. This term may be a Factor, a BoundColumn or
        a Slice. If `target` is two-dimensional, correlations are computed
        asset-wise.
    correlation_length : int
        Length of the lookback window over which to compute each correlation
        coefficient.
    mask : ziplime.pipeline.Filter, optional
        A Filter describing which assets (columns) of `base_factor` should have
        their correlation with `target` computed each day.

    See Also
    --------
    :func:`scipy.stats.spearmanr`
    :meth:`Factor.spearmanr`
    :class:`ziplime.pipeline.factors.RollingSpearmanOfReturns`

    Notes
    -----
    Most users should call Factor.spearmanr rather than directly construct an
    instance of this class.
    """

    window_safe = True

    def compute(self, today, assets, out, base_data, target_data):
        # If `target_data` is a Slice or single column of data, broadcast it
        # out to the same shape as `base_data`, then compute column-wise. This
        # is efficient because each column of the broadcasted array only refers
        # to a single memory location.
        target_data = broadcast_arrays(target_data, base_data)[0]
        for i in range(len(out)):
            out[i] = spearmanr(base_data[:, i], target_data[:, i])[0]
