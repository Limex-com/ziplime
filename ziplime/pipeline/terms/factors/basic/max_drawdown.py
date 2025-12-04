import numpy as np
from numpy import fmax, isnan

from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.utils.math_utils import nanargmax, nanmax


class MaxDrawdown(SingleInputMixin, CustomFactor):
    """
    Max Drawdown

    **Default Inputs:** None

    **Default Window Length:** None
    """

    # ctx = ignore_nanwarnings()

    def compute(self, today, assets, out, data):
        drawdowns = fmax.accumulate(data, axis=0) - data
        drawdowns[isnan(drawdowns)] = -np.inf
        drawdown_ends = nanargmax(drawdowns, axis=0)

        # TODO: Accelerate this loop in Cython or Numba.
        for i, end in enumerate(drawdown_ends):
            peak = nanmax(data[: end + 1, i])
            out[i] = (peak - data[end, i]) / data[end, i]
