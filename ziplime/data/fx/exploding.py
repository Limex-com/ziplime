import numpy as np
import pandas as pd

from ziplime.data.fx.base import FXRateReader


class ExplodingFXRateReader(FXRateReader):
    """An FXRateReader that raises an error when used.

    This is useful for testing contexts where FX rates aren't actually needed.
    """

    def get_rates(self, rate: str, quote: str, bases: np.ndarray, dts: pd.DatetimeIndex):
        raise AssertionError("FX rates requested unexpectedly!")
