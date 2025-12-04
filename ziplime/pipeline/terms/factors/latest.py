from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.pipeline.mixins import LatestMixin


class Latest(LatestMixin, CustomFactor):
    """
    Factor producing the most recently-known value of `inputs[0]` on each day.

    The `.latest` attribute of DataSet columns returns an instance of this
    Factor.
    """

    window_length = 1

    def compute(self, today, assets, out, data):
        out[:] = data[-1]

