from numpy import arange

from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.utils.math_utils import nansum
from ziplime.utils.numpy_utils import float64_dtype


class LinearWeightedMovingAverage(SingleInputMixin, CustomFactor):
    """
    Weighted Average Value of an arbitrary column

    **Default Inputs**: None

    **Default Window Length**: None
    """

    # numpy's nan functions throw warnings when passed an array containing only
    # nans, but they still returns the desired value (nan), so we ignore the
    # warning.
    # ctx = ignore_nanwarnings()

    def compute(self, today, assets, out, data):
        ndays = data.shape[0]

        # Initialize weights array
        weights = arange(1, ndays + 1, dtype=float64_dtype).reshape(ndays, 1)

        # Compute normalizer
        normalizer = (ndays * (ndays + 1)) / 2

        # Weight the data
        weighted_data = data * weights

        # Compute weighted averages
        out[:] = nansum(weighted_data, axis=0) / normalizer
