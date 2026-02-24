from ziplime.lib.labelarray import LabelArray
from ziplime.pipeline.mixins import SingleInputMixin, StandardOutputs
from ziplime.pipeline.terms.filters.custom_filter import CustomFilter
from ziplime.pipeline.terms.filters.filter import Filter
from numpy import (
    any as np_any,
)
from ziplime.utils.numpy_utils import is_missing


class AllPresent(CustomFilter, SingleInputMixin, StandardOutputs):
    """Pipeline filter indicating input term has data for a given window."""

    def _validate(self):

        if isinstance(self.inputs[0], Filter):
            raise TypeError("Input to filter `AllPresent` cannot be a Filter.")

        return super(AllPresent, self)._validate()

    def compute(self, today, assets, out, value):
        if isinstance(value, LabelArray):
            out[:] = ~np_any(value.is_missing(), axis=0)
        else:
            out[:] = ~np_any(
                is_missing(value, self.inputs[0].missing_value),
                axis=0,
            )

