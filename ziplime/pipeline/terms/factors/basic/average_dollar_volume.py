from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.utils.math_utils import nansum


class AverageDollarVolume(CustomFactor):
    """
    Average Daily Dollar Volume

    **Default Inputs:** [EquityPricing.close, EquityPricing.volume]

    **Default Window Length:** None
    """

    inputs = [EquityPricing.close, EquityPricing.volume]

    def compute(self, today, assets, out, close, volume):
        out[:] = nansum(close * volume, axis=0) / len(close)
