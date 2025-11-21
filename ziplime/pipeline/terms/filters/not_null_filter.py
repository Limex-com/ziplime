from ziplime.lib.labelarray import LabelArray
from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.filters import Filter
from ziplime.utils.numpy_utils import is_missing


class NotNullFilter(SingleInputMixin, Filter):
    """
    A Filter indicating whether input values are **not** missing from an input.

    Parameters
    ----------
    factor : ziplime.pipeline.Term
        The factor to compare against its missing_value.
    """

    window_length = 0

    def __new__(cls, term):
        return super(NotNullFilter, cls).__new__(
            cls,
            inputs=(term,),
        )

    def _compute(self, arrays, dates, assets, mask):
        data = arrays[0]
        if isinstance(data, LabelArray):
            return ~data.is_missing()
        return ~is_missing(arrays[0], self.inputs[0].missing_value)
