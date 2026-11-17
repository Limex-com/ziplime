from numpy import errstate as np_errstate
import numpy as np
import pandas as pd
from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor


class PercentChange(SingleInputMixin, CustomFactor):
    """
    Calculates the percent change over the given window_length.

    **Default Inputs:** None

    **Default Window Length:** None

    Notes
    -----
    Percent change is calculated as ``(new - old) / abs(old)``.
    """

    window_safe = True

    def _validate(self) -> None:
        super(PercentChange, self)._validate()
        if self.window_length < 2:
            raise ValueError(
                "'PercentChange' expected a window length"
                "of at least 2, but was given {window_length}. "
                "For daily percent change, use a window "
                "length of 2.".format(window_length=self.window_length)
            )

    def compute(self, today: 'pd.Timestamp', assets: 'pd.Index', out: 'np.ndarray', values: 'np.ndarray') -> None:
        with np_errstate(divide="ignore", invalid="ignore"):
            out[:] = (values[-1] - values[0]) / abs(values[0])
