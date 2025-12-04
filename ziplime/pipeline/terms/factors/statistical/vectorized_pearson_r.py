from numexpr import evaluate
import numpy as np
from ziplime.utils.math_utils import nanmean


def vectorized_pearson_r(dependents, independents, allowed_missing, out=None):
    """Compute Pearson's r between columns of ``dependents`` and ``independents``.

    Parameters
    ----------
    dependents : np.array[N, M]
        Array with columns of data to be regressed against ``independent``.
    independents : np.array[N, M] or np.array[N, 1]
        Independent variable(s) of the regression. If a single column is
        passed, it is broadcast to the shape of ``dependents``.
    allowed_missing : int
        Number of allowed missing (NaN) observations per column. Columns with
        more than this many non-nan observations in either ``dependents`` or
        ``independents`` will output NaN as the correlation coefficient.
    out : np.array[M] or None, optional
        Output array into which to write results.  If None, a new array is
        created and returned.

    Returns
    -------
    correlations : np.array[M]
        Pearson correlation coefficients for each column of ``dependents``.

    See Also
    --------
    :class:`ziplime.pipeline.factors.RollingPearson`
    :class:`ziplime.pipeline.factors.RollingPearsonOfReturns`
    """
    nan = np.nan
    isnan = np.isnan
    N, M = dependents.shape

    if out is None:
        out = np.full(M, nan)

    if allowed_missing > 0:
        # If we're handling nans robustly, we need to mask both arrays to
        # locations where either was nan.
        either_nan = isnan(dependents) | isnan(independents)
        independents = np.where(either_nan, nan, independents)
        dependents = np.where(either_nan, nan, dependents)
        mean = nanmean
    else:
        # Otherwise, we can just use mean, which will give us a nan for any
        # column where there's ever a nan.
        mean = np.mean

    # Pearson R is Cov(X, Y) / StdDev(X) * StdDev(Y)
    # c.f. https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
    ind_residual = independents - mean(independents, axis=0)
    dep_residual = dependents - mean(dependents, axis=0)

    ind_variance = mean(ind_residual ** 2, axis=0)
    dep_variance = mean(dep_residual ** 2, axis=0)

    covariances = mean(ind_residual * dep_residual, axis=0)

    evaluate(
        "where(mask, nan, cov / sqrt(ind_variance * dep_variance))",
        local_dict={
            "cov": covariances,
            "mask": isnan(independents).sum(axis=0) > allowed_missing,
            "nan": np.nan,
            "ind_variance": ind_variance,
            "dep_variance": dep_variance,
        },
        global_dict={},
        out=out,
    )
    return out
