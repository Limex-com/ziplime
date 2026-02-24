from numpy import (
    abs,
    clip,
    diff,
    inf,
)
from numexpr import evaluate

from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.utils.math_utils import nanmean


class RSI(SingleInputMixin, CustomFactor):
    """
    Relative Strength Index

    **Default Inputs**: :data:`ziplime.pipeline.data.EquityPricing.close`

    **Default Window Length**: 15
    """

    window_length = 15
    inputs = (EquityPricing.close,)
    window_safe = True

    def compute(self, today, assets, out, closes):
        diffs = diff(closes, axis=0)
        ups = nanmean(clip(diffs, 0, inf), axis=0)
        downs = abs(nanmean(clip(diffs, -inf, 0), axis=0))
        return evaluate(
            "100 - (100 / (1 + (ups / downs)))",
            local_dict={"ups": ups, "downs": downs},
            global_dict={},
            out=out,
        )
