from numexpr import evaluate

from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.utils.math_utils import nanmax, nanmin


class FastStochasticOscillator(CustomFactor):
    """
    Fast Stochastic Oscillator Indicator [%K, Momentum Indicator]
    https://wiki.timetotrade.eu/Stochastic

    This stochastic is considered volatile, and varies a lot when used in
    market analysis. It is recommended to use the slow stochastic oscillator
    or a moving average of the %K [%D].

    **Default Inputs:** :data:`ziplime.pipeline.data.EquityPricing.close`, \
                        :data:`ziplime.pipeline.data.EquityPricing.low`, \
                        :data:`ziplime.pipeline.data.EquityPricing.high`

    **Default Window Length:** 14

    Returns
    -------
    out: %K oscillator
    """

    inputs = (EquityPricing.close, EquityPricing.low, EquityPricing.high)
    window_safe = True
    window_length = 14

    def compute(self, today, assets, out, closes, lows, highs):

        highest_highs = nanmax(highs, axis=0)
        lowest_lows = nanmin(lows, axis=0)
        today_closes = closes[-1]

        evaluate(
            "((tc - ll) / (hh - ll)) * 100",
            local_dict={
                "tc": today_closes,
                "ll": lowest_lows,
                "hh": highest_highs,
            },
            global_dict={},
            out=out,
        )

