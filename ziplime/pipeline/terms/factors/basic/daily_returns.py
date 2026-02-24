from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors.basic.returns import Returns


class DailyReturns(Returns):
    """
    Calculates daily percent change in close price.

    **Default Inputs**: [EquityPricing.close]
    """

    inputs = [EquityPricing.close]
    window_safe = True
    window_length = 2
