from numpy import abs, dstack

from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.utils.math_utils import nanmax


class TrueRange(CustomFactor):
    """
    True Range

    A technical indicator originally developed by J. Welles Wilder, Jr.
    Indicates the true degree of daily price change in an underlying.

    **Default Inputs:** :data:`ziplime.pipeline.data.EquityPricing.high`, \
                        :data:`ziplime.pipeline.data.EquityPricing.low`, \
                        :data:`ziplime.pipeline.data.EquityPricing.close`

    **Default Window Length:** 2
    """

    inputs = (
        EquityPricing.high,
        EquityPricing.low,
        EquityPricing.close,
    )
    window_length = 2

    def compute(self, today, assets, out, highs, lows, closes):
        high_to_low = highs[1:] - lows[1:]
        high_to_prev_close = abs(highs[1:] - closes[:-1])
        low_to_prev_close = abs(lows[1:] - closes[:-1])
        out[:] = nanmax(
            dstack(
                (
                    high_to_low,
                    high_to_prev_close,
                    low_to_prev_close,
                )
            ),
            2,
        )
