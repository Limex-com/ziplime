from ziplime.lib.labelarray import LabelArray
from ziplime.pipeline.mixins import SingleInputMixin, StandardOutputs
from ziplime.pipeline.terms.filters.custom_filter import CustomFilter
from ziplime.pipeline.terms.filters.filter import Filter
from numpy import (
    any as np_any,
)
import numpy as np
import pandas as pd
from ziplime.utils.numpy_utils import is_missing


class AllPresent(CustomFilter, SingleInputMixin, StandardOutputs):
    """Pipeline filter indicating input term has data for a given window."""

    def _validate(self):

        if isinstance(self.inputs[0], Filter):
            raise TypeError("Input to filter `AllPresent` cannot be a Filter.")

        return super(AllPresent, self)._validate()

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray, value: np.ndarray) -> None:
        if isinstance(value, LabelArray):
            out[:] = ~np_any(value.is_missing(), axis=0)
        else:
            out[:] = ~np_any(
                is_missing(value, self.inputs[0].missing_value),
                axis=0,
            )
