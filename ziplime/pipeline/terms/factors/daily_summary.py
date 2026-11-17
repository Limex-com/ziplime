from collections.abc import Callable

from numpy import isnan, nan
import numpy as np
import pandas as pd

from ziplime.pipeline.terms.factors.factor import Factor

from ziplime.pipeline.mixins import (
    SingleInputMixin,
)

from ziplime.utils.numpy_utils import (
    as_column,
    float64_dtype,
)
from ziplime.pipeline.terms.filters.filter import Filter

_RANK_METHODS = frozenset(["average", "min", "max", "dense", "ordinal"])


class DailySummary(SingleInputMixin, Factor):
    """1D Factor that computes a summary statistic across all assets."""

    ndim = 1
    window_length = 0
    params = ("func",)

    def __new__(cls, func: Callable, input_: Factor, mask: Filter | None, dtype: np.dtype):
        # TODO: We should be able to support datetime64 as well, but that
        # requires extra care for handling NaT.
        if dtype != float64_dtype:
            raise AssertionError(
                "DailySummary only supports float64 dtype, got {}".format(dtype),
            )

        return super(DailySummary, cls).__new__(
            cls,
            inputs=[input_],
            dtype=dtype,
            missing_value=nan,
            window_safe=input_.window_safe,
            func=func,
            mask=mask,
        )

    def _compute(self, arrays: list[np.ndarray], dates: pd.DatetimeIndex, assets: pd.Index, mask: np.ndarray):
        func = self.params["func"]

        data = arrays[0]
        data[~mask] = nan
        if not isnan(self.inputs[0].missing_value):
            data[data == self.inputs[0].missing_value] = nan

        return as_column(func(data, self.inputs[0].missing_value))

    def __repr__(self) -> str:
        return "{}.{}()".format(
            self.inputs[0].recursive_repr(),
            self.params["func"].__name__,
        )

    graph_repr = recursive_repr = __repr__
