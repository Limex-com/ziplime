from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors.basic.weighted_average_value import WeightedAverageValue


class VWAP(WeightedAverageValue):
    """
    Volume Weighted Average Price

    **Default Inputs:** [EquityPricing.close, EquityPricing.volume]

    **Default Window Length:** None
    """

    inputs = (EquityPricing.close, EquityPricing.volume)
