from numpy import where, zeros
import numpy as np
import pandas as pd

from ziplime.pipeline.terms.classifiers.classifier import Classifier
from ziplime.utils.numpy_utils import (
    int64_dtype,
)


class Everything(Classifier):
    """
    A trivial classifier that classifies everything the same.
    """

    dtype = int64_dtype
    window_length = 0
    inputs = ()
    missing_value = -1

    def _compute(self, arrays: 'list[np.ndarray]', dates: 'pd.DatetimeIndex', assets: 'pd.Index', mask: 'np.ndarray'):
        return where(
            mask,
            zeros(shape=mask.shape, dtype=int64_dtype),
            self.missing_value,
        )
