from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.utils.math_utils import nanmean, nanstd


class BollingerBands(CustomFactor):
    """
    Bollinger Bands technical indicator.
    https://en.wikipedia.org/wiki/Bollinger_Bands

    **Default Inputs:** :data:`ziplime.pipeline.data.EquityPricing.close`

    Parameters
    ----------
    inputs : length-1 iterable[BoundColumn]
        The expression over which to compute bollinger bands.
    window_length : int > 0
        Length of the lookback window over which to compute the bollinger
        bands.
    k : float
        The number of standard deviations to add or subtract to create the
        upper and lower bands.
    """

    params = ("k",)
    inputs = (EquityPricing.close,)
    outputs = "lower", "middle", "upper"

    def compute(self, today, assets, out, close, k):
        difference = k * nanstd(close, axis=0)
        out.middle = middle = nanmean(close, axis=0)
        out.upper = middle + difference
        out.lower = middle - difference
