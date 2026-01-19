from ziplime.pipeline.terms.filters import Filter
from ziplime.utils.numpy_utils import repeat_first_axis


class StaticSids(Filter):
    """
    A Filter that computes True for a specific set of predetermined sids.

    ``StaticSids`` is mostly useful for debugging or for interactively
    computing pipeline terms for a fixed set of sids that are known ahead of
    time.

    Parameters
    ----------
    sids : iterable[int]
        An iterable of sids for which to filter.
    """

    inputs = ()
    window_length = 0
    params = ("sids",)

    def __new__(cls, sids):
        sids = frozenset(sids)
        return super(StaticSids, cls).__new__(cls, sids=sids)

    def _compute(self, arrays, dates, sids, mask):
        my_columns = sids.isin(self.params["sids"])
        return repeat_first_axis(my_columns, len(mask)) & mask
