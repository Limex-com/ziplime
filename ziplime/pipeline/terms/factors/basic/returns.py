import numpy as np
import pandas as pd

from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.pipeline.data import EquityPricing


class Returns(CustomFactor):
    """
    Calculates the percent change in close price over the given window_length.

    **Default Inputs**: [EquityPricing.close]
    """

    inputs = [EquityPricing.close]
    window_safe = True

    def _validate(self) -> None:
        super(Returns, self)._validate()
        if self.window_length < 2:
            raise ValueError(
                "'Returns' expected a window length of at least 2, but was "
                "given {window_length}. For daily returns, use a window "
                "length of 2.".format(window_length=self.window_length)
            )

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray, close: np.ndarray) -> None:
        out[:] = (close[-1] - close[0]) / close[0]
