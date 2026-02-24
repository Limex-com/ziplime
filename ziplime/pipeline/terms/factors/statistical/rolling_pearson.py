from ziplime.pipeline.terms.factors.statistical.rolling_correlation import RollingCorrelation
from ziplime.pipeline.terms.factors.statistical import vectorized_pearson_r


class RollingPearson(RollingCorrelation):
    """
    A Factor that computes pearson correlation coefficients between the columns
    of a given Factor and either the columns of another Factor/BoundColumn or a
    slice/single column of data.

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
    :func:`scipy.stats.pearsonr`
    :meth:`Factor.pearsonr`
    :class:`ziplime.pipeline.factors.RollingPearsonOfReturns`

    Notes
    -----
    Most users should call Factor.pearsonr rather than directly construct an
    instance of this class.
    """

    window_safe = True

    def compute(self, today, assets, out, base_data, target_data):
        vectorized_pearson_r(
            base_data,
            target_data,
            allowed_missing=0,
            out=out,
        )
