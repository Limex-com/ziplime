import numpy as np
from ziplime.utils.math_utils import nanmean


def vectorized_beta(dependents, independent, allowed_missing, out=None):
    """Compute slopes of linear regressions between columns of ``dependents`` and
    ``independent``.

    Parameters
    ----------
    dependents : np.array[N, M]
        Array with columns of data to be regressed against ``independent``.
    independent : np.array[N, 1]
        Independent variable of the regression
    allowed_missing : int
        Number of allowed missing (NaN) observations per column. Columns with
        more than this many non-nan observations in either ``dependents`` or
        ``independents`` will output NaN as the regression coefficient.
    out : np.array[M] or None, optional
        Output array into which to write results.  If None, a new array is
        created and returned.

    Returns
    -------
    slopes : np.array[M]
        Linear regression coefficients for each column of ``dependents``.
    """
    # Cache these as locals since we're going to call them multiple times.
    nan = np.nan
    isnan = np.isnan
    N, M = dependents.shape

    if out is None:
        out = np.full(M, nan)

    # Copy N times as a column vector and fill with nans to have the same
    # missing value pattern as the dependent variable.
    #
    # PERF_TODO: We could probably avoid the space blowup by doing this in
    # Cython.

    # shape: (N, M)
    independent = np.where(
        isnan(dependents),
        nan,
        independent,
    )

    # Calculate beta as Cov(X, Y) / Cov(X, X).
    # https://en.wikipedia.org/wiki/Simple_linear_regression#Fitting_the_regression_line  # noqa
    #
    # NOTE: The usual formula for covariance is::
    #
    #    mean((X - mean(X)) * (Y - mean(Y)))
    #
    # However, we don't actually need to take the mean of both sides of the
    # product, because of the folllowing equivalence::
    #
    # Let X_res = (X - mean(X)).
    # We have:
    #
    #     mean(X_res * (Y - mean(Y))) = mean(X_res * (Y - mean(Y)))
    #                             (1) = mean((X_res * Y) - (X_res * mean(Y)))
    #                             (2) = mean(X_res * Y) - mean(X_res * mean(Y))
    #                             (3) = mean(X_res * Y) - mean(X_res) * mean(Y)
    #                             (4) = mean(X_res * Y) - 0 * mean(Y)
    #                             (5) = mean(X_res * Y)
    #
    #
    # The tricky step in the above derivation is step (4). We know that
    # mean(X_res) is zero because, for any X:
    #
    #     mean(X - mean(X)) = mean(X) - mean(X) = 0.
    #
    # The upshot of this is that we only have to center one of `independent`
    # and `dependent` when calculating covariances. Since we need the centered
    # `independent` to calculate its variance in the next step, we choose to
    # center `independent`.

    # shape: (N, M)
    ind_residual = independent - nanmean(independent, axis=0)

    # shape: (M,)
    covariances = nanmean(ind_residual * dependents, axis=0)

    # We end up with different variances in each column here because each
    # column may have a different subset of the data dropped due to missing
    # data in the corresponding dependent column.
    # shape: (M,)
    independent_variances = nanmean(ind_residual ** 2, axis=0)

    # shape: (M,)
    np.divide(covariances, independent_variances, out=out)

    # Write nans back to locations where we have more then allowed number of
    # missing entries.
    nanlocs = isnan(independent).sum(axis=0) > allowed_missing
    out[nanlocs] = nan

    return out
