from ziplime.lib.labelarray import LabelArray
from ziplime.pipeline.terms.classifiers import Classifier
from ziplime.utils.numpy_utils import (
    categorical_dtype,
)

from ziplime.pipeline.mixins import (
    SingleInputMixin,
)


class Relabel(SingleInputMixin, Classifier):
    """
    A classifier applying a relabeling function on the result of another
    classifier.

    Parameters
    ----------
    arg : ziplime.pipeline.Classifier
        Term produceing the input to be relabeled.
    relabel_func : function(LabelArray) -> LabelArray
        Function to apply to the result of `term`.
    """

    window_length = 0
    params = ("relabeler",)

    # TODO: Support relabeling for integer dtypes.
    def __new__(cls, term: categorical_dtype, relabeler):
        return super(Relabel, cls).__new__(
            cls,
            inputs=(term,),
            dtype=term.dtype,
            mask=term.mask,
            relabeler=relabeler,
        )

    def _compute(self, arrays, dates, assets, mask):
        relabeler = self.params["relabeler"]
        data = arrays[0]

        if isinstance(data, LabelArray):
            result = data.map(relabeler)
            result[~mask] = data.missing_value
        else:
            raise NotImplementedError(
                "Relabeling is not currently supported for " "int-dtype classifiers."
            )
        return result
