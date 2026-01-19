from ziplime.pipeline.mixins import LatestMixin
from ziplime.pipeline.terms.filters.custom_filter import CustomFilter


class Latest(LatestMixin, CustomFilter):
    """
    Filter producing the most recently-known value of `inputs[0]` on each day.
    """

    pass
