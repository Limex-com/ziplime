from ziplime.pipeline.mixins import LatestMixin
from ziplime.pipeline.terms.classifiers.custom_classifier import CustomClassifier


class Latest(LatestMixin, CustomClassifier):
    """
    A classifier producing the latest value of an input.

    See Also
    --------
    ziplime.pipeline.data.dataset.BoundColumn.latest
    """

    pass
