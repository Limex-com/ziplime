"""Utils for labelarray."""

import numpy as np
from ziplime.lib.labelarray.label_array import LabelArray


def labelarray_where(cond: np.ndarray, trues: LabelArray, falses: LabelArray) -> LabelArray:
    """LabelArray-aware implementation of np.where."""
    if trues.missing_value != falses.missing_value:
        raise ValueError("Can't compute where on arrays with different missing values.")

    strs = np.where(cond, trues.as_string_array(), falses.as_string_array())
    return LabelArray(strs, missing_value=trues.missing_value)
