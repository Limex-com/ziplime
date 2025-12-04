from ziplime.utils.math_utils import nanmean

from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor


class SimpleMovingAverage(SingleInputMixin, CustomFactor):
    """
    Average Value of an arbitrary column

    **Default Inputs**: None

    **Default Window Length**: None
    """

    # numpy's nan functions throw warnings when passed an array containing only
    # nans, but they still returns the desired value (nan), so we ignore the
    # warning.
    # ctx = ignore_nanwarnings()

    def compute(self, today, assets, out, data):
        out[:] = nanmean(data, axis=0)
