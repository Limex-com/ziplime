import numpy as np
import pandas as pd

from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.pipeline.mixins import LatestMixin


class Latest(LatestMixin, CustomFactor):
    """
    Factor producing the most recently-known value of `inputs[0]` on each day.

    The `.latest` attribute of DataSet columns returns an instance of this
    Factor.
    """

    window_length = 1

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray, data: np.ndarray) -> None:
        out[:] = data[-1]
