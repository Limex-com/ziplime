from numexpr import evaluate
import numpy as np
import pandas as pd

from ziplime.pipeline.data import EquityPricing
from ziplime.pipeline.terms.factors import CustomFactor
from ziplime.utils.math_utils import nanargmax, nanargmin


class Aroon(CustomFactor):
    """
    Aroon technical indicator.
    https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/aroon-indicator

    **Defaults Inputs:** :data:`ziplime.pipeline.data.EquityPricing.low`, \
                         :data:`ziplime.pipeline.data.EquityPricing.high`

    Parameters
    ----------
    window_length : int > 0
        Length of the lookback window over which to compute the Aroon
        indicator.
    """  # noqa

    inputs = (EquityPricing.low, EquityPricing.high)
    outputs = ("down", "up")

    def compute(
        self,
        today: 'pd.Timestamp',
        assets: 'pd.Index',
        out: 'np.ndarray',
        lows: 'np.ndarray',
        highs: 'np.ndarray',
    ) -> None:
        wl = self.window_length
        high_date_index = nanargmax(highs, axis=0)
        low_date_index = nanargmin(lows, axis=0)
        evaluate(
            "(100 * high_date_index) / (wl - 1)",
            local_dict={
                "high_date_index": high_date_index,
                "wl": wl,
            },
            out=out.up,
        )
        evaluate(
            "(100 * low_date_index) / (wl - 1)",
            local_dict={
                "low_date_index": low_date_index,
                "wl": wl,
            },
            out=out.down,
        )
