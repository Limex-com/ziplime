from numexpr import evaluate

from ziplime.pipeline.terms.factors import CustomFactor


class RateOfChangePercentage(CustomFactor):
    """
    Rate of change Percentage
    ROC measures the percentage change in price from one period to the next.
    The ROC calculation compares the current price with the price `n`
    periods ago.
    Formula for calculation: ((price - prevPrice) / prevPrice) * 100
    price - the current price
    prevPrice - the price n days ago, equals window length
    """

    def compute(self, today, assets, out, close):
        today_close = close[-1]
        prev_close = close[0]
        evaluate(
            "((tc - pc) / pc) * 100",
            local_dict={"tc": today_close, "pc": prev_close},
            global_dict={},
            out=out,
        )
