import numpy as np
from numpy import broadcast_arrays
from scipy.stats import (
    linregress,
)

from ziplime.errors import IncompatibleTerms
from ziplime.pipeline.terms.factors import CustomFactor




class RollingLinearRegression(CustomFactor):
    """
    A Factor that performs an ordinary least-squares regression predicting the
    columns of a given Factor from either the columns of another
    Factor/BoundColumn or a slice/single column of data.

    Parameters
    ----------
    dependent : ziplime.pipeline.Factor
        The factor whose columns are the predicted/dependent variable of each
        regression with `independent`.
    independent : ziplime.pipeline.slice.Slice or ziplime.pipeline.Factor
        The factor/slice whose columns are the predictor/independent variable
        of each regression with `dependent`. If `independent` is a Factor,
        regressions are computed asset-wise.
    regression_length : int
        Length of the lookback window over which to compute each regression.
    mask : ziplime.pipeline.Filter, optional
        A Filter describing which assets (columns) of `dependent` should be
        regressed against `independent` each day.

    See Also
    --------
    :func:`scipy.stats.linregress`
    :meth:`Factor.linear_regression`
    :class:`ziplime.pipeline.factors.RollingLinearRegressionOfReturns`

    Notes
    -----
    Most users should call Factor.linear_regression rather than directly
    construct an instance of this class.
    """

    outputs = ["alpha", "beta", "r_value", "p_value", "stderr"]

    def __new__(cls, dependent: np.float64 | np.int64, independent: np.float64 | np.int64,
                regression_length, mask=None):
        if independent.ndim == 2 and dependent.mask is not independent.mask:
            raise IncompatibleTerms(term_1=dependent, term_2=independent)
        if regression_length < 2:
            raise ValueError("regression_length must be greater than or equal to 2")
        return super(RollingLinearRegression, cls).__new__(
            cls,
            inputs=[dependent, independent],
            window_length=regression_length,
            mask=mask,
        )

    def compute(self, today, assets, out, dependent, independent):
        alpha = out.alpha
        beta = out.beta
        r_value = out.r_value
        p_value = out.p_value
        stderr = out.stderr

        def regress(y, x):
            regr_results = linregress(y=y, x=x)
            # `linregress` returns its results in the following order:
            # slope, intercept, r-value, p-value, stderr
            alpha[i] = regr_results[1]
            beta[i] = regr_results[0]
            r_value[i] = regr_results[2]
            p_value[i] = regr_results[3]
            stderr[i] = regr_results[4]

        # If `independent` is a Slice or single column of data, broadcast it
        # out to the same shape as `dependent`, then compute column-wise. This
        # is efficient because each column of the broadcasted array only refers
        # to a single memory location.
        independent = broadcast_arrays(independent, dependent)[0]
        for i in range(len(out)):
            regress(y=dependent[:, i], x=independent[:, i])
