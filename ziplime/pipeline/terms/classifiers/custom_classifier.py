from ziplime.errors import UnsupportedDataType
from ziplime.pipeline.dtypes import (
    FACTOR_DTYPES,
    FILTER_DTYPES,
)
from ziplime.pipeline.terms.classifiers.classifier import Classifier
from ziplime.utils.numpy_utils import (
    int64_dtype,
)

from ziplime.pipeline.mixins import (
    CustomTermMixin,
    PositiveWindowLengthMixin,
    StandardOutputs,
)


class CustomClassifier(
    PositiveWindowLengthMixin, StandardOutputs, CustomTermMixin, Classifier
):
    """
    Base class for user-defined Classifiers.

    Does not suppport multiple outputs.

    See Also
    --------
    ziplime.pipeline.CustomFactor
    ziplime.pipeline.CustomFilter
    """

    def _validate(self):
        try:
            super(CustomClassifier, self)._validate()
        except UnsupportedDataType as exc:
            if self.dtype in FACTOR_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomFactor?",
                ) from exc
            elif self.dtype in FILTER_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomFilter?",
                ) from exc
            raise

    def _allocate_output(self, windows, shape):
        """
        Override the default array allocation to produce a LabelArray when we
        have a string-like dtype.
        """
        if self.dtype == int64_dtype:
            return super(CustomClassifier, self)._allocate_output(
                windows,
                shape,
            )

        # This is a little bit of a hack.  We might not know what the
        # categories for a LabelArray are until it's actually been loaded, so
        # we need to look at the underlying data.
        return windows[0].data.empty_like(shape)
