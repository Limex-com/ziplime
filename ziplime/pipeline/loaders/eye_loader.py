import numpy as np

from ziplime.pipeline.loaders.precomputed_loader import PrecomputedLoader


class EyeLoader(PrecomputedLoader):
    """A PrecomputedLoader that emits arrays containing 1s on the diagonal and 0s
    elsewhere.

    Parameters
    ----------
    columns : list[BoundColumn]
        Columns that this loader should know about.
    dates : iterable[datetime-like]
        Same as PrecomputedLoader.
    sids : iterable[int-like]
        Same as PrecomputedLoader
    """

    def __init__(self, columns, dates, sids):
        shape = (len(dates), len(sids))
        super(EyeLoader, self).__init__(
            {column: np.eye(shape, dtype=column.dtype) for column in columns},
            dates,
            sids,
        )
