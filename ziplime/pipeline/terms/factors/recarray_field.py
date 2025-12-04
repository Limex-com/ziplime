from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.pipeline.mixins import SingleInputMixin


class RecarrayField(SingleInputMixin, Factor):
    """
    A single field from a multi-output factor.
    """

    def __new__(cls, factor, attribute):
        return super(RecarrayField, cls).__new__(
            cls,
            attribute=attribute,
            inputs=[factor],
            window_length=0,
            mask=factor.mask,
            dtype=factor.dtype,
            missing_value=factor.missing_value,
            window_safe=factor.window_safe,
        )

    def _init(self, attribute, *args, **kwargs):
        self._attribute = attribute
        return super(RecarrayField, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, attribute, *args, **kwargs):
        return (
            super(RecarrayField, cls)._static_identity(*args, **kwargs),
            attribute,
        )

    def _compute(self, windows, dates, assets, mask):
        return windows[0][self._attribute]

    def graph_repr(self):
        return "{}.{}".format(self.inputs[0].recursive_repr(), self._attribute)
