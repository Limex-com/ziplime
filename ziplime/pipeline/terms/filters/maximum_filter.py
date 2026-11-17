from numpy import (
    uint8,
)
import numpy as np
import pandas as pd
from ziplime.lib.rank import is_missing, grouped_masked_is_maximal

from ziplime.pipeline.mixins import (
    StandardOutputs,
)
from ziplime.pipeline.terms.classifiers.classifier import Classifier
from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.pipeline.terms.filters.filter import Filter
from ziplime.utils.numpy_utils import (
    int64_dtype,
)


class MaximumFilter(Filter, StandardOutputs):
    """Pipeline filter that selects the top asset, possibly grouped and masked."""

    window_length = 0

    def __new__(cls, factor: Factor, groupby: Classifier | None, mask: Filter | None):
        if groupby is None:
            from ziplime.pipeline.terms.classifiers.everything import Everything

            groupby = Everything()

        return super(MaximumFilter, cls).__new__(
            cls,
            inputs=(factor, groupby),
            mask=mask,
        )

    def _compute(self, arrays: list[np.ndarray], dates: pd.DatetimeIndex, assets: pd.Index, mask: np.ndarray):
        # XXX: We're doing a lot of unncessary work here if `groupby` isn't
        # specified.
        data = arrays[0]
        group_labels, null_label = self.inputs[1]._to_integral(arrays[1])
        effective_mask = (
            mask
            & (group_labels != null_label)
            & ~is_missing(data, self.inputs[0].missing_value)
        ).view(uint8)

        return grouped_masked_is_maximal(
            # Unconditionally view the data as int64.
            # This is safe because casting from float64 to int64 is an
            # order-preserving operation.
            data.view(int64_dtype),
            # PERF: Consider supporting different sizes of group labels.
            group_labels.astype(int64_dtype),
            effective_mask,
        )

    def __repr__(self) -> str:
        return "Maximum({}, groupby={}, mask={})".format(
            self.inputs[0].recursive_repr(),
            self.inputs[1].recursive_repr(),
            self.mask.recursive_repr(),
        )

    def graph_repr(self) -> str:
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "Maximum:\\l  groupby: {}\\l  mask: {}\\l".format(
            self.inputs[1].recursive_repr(),
            self.mask.recursive_repr(),
        )
