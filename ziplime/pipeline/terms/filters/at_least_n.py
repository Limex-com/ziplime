import numpy as np
import pandas as pd

from ziplime.pipeline.terms.filters.custom_filter import CustomFilter


class AtLeastN(CustomFilter):
    """
    A Filter requiring that assets produce True for at least N days in the
    last ``window_length`` days.

    **Default Inputs:** None

    **Default Window Length:** None
    """

    params = ("N",)

    def compute(self, today: 'pd.Timestamp', assets: 'pd.Index', out: 'np.ndarray', arg: 'np.ndarray', N: int) -> None:
        out[:] = arg.sum(axis=0) >= N
