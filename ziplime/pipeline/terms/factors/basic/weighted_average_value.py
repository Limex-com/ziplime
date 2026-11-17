import numpy as np
import pandas as pd

from ziplime.pipeline.terms.factors.custom_factor import CustomFactor
from ziplime.utils.math_utils import nansum


class WeightedAverageValue(CustomFactor):
    """
    Helper for VWAP-like computations.

    **Default Inputs:** None

    **Default Window Length:** None
    """

    def compute(
        self,
        today: 'pd.Timestamp',
        assets: 'pd.Index',
        out: 'np.ndarray',
        base: 'np.ndarray',
        weight: 'np.ndarray',
    ) -> None:
        out[:] = nansum(base * weight, axis=0) / nansum(weight, axis=0)
