
from numpy.random import RandomState
from pandas import Timestamp


from ziplime.utils.numpy_utils import (
    bool_dtype,
    datetime64ns_dtype,
    float64_dtype,
    int64_dtype,
    object_dtype,
)
from .precomputed_loader import PrecomputedLoader
from ...assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository


class SeededRandomLoader(PrecomputedLoader):
    """A PrecomputedLoader that emits arrays randomly-generated with a given seed.

    Parameters
    ----------
    seed : int
        Seed for numpy.random.RandomState.
    columns : list[BoundColumn]
        Columns that this loader should know about.
    dates : iterable[datetime-like]
        Same as PrecomputedLoader.
    sids : iterable[int-like]
        Same as PrecomputedLoader
    """

    def __init__(self, seed, columns, dates, sids):
        self._seed = seed
        super(SeededRandomLoader, self).__init__(
            {c: self.values(c.dtype, dates, sids) for c in columns},
            dates,
            sids,
        )

    def values(self, dtype, dates, sids):
        """Make a random array of shape (len(dates), len(sids)) with ``dtype``."""
        shape = (len(dates), len(sids))
        return {
            datetime64ns_dtype: self._datetime_values,
            float64_dtype: self._float_values,
            int64_dtype: self._int_values,
            bool_dtype: self._bool_values,
            object_dtype: self._object_values,
        }[dtype](shape)

    @property
    def state(self):
        """Make a new RandomState from our seed.

        This ensures that every call to _*_values produces the same output
        every time for a given SeededRandomLoader instance.
        """
        return RandomState(self._seed)

    def _float_values(self, shape):
        """Return uniformly-distributed floats between -0.0 and 100.0."""
        return self.state.uniform(low=0.0, high=100.0, size=shape)

    def _int_values(self, shape):
        """
        Return uniformly-distributed integers between 0 and 100.
        """
        return self.state.randint(low=0, high=100, size=shape).astype(
            "int64"
        )  # default is system int

    def _datetime_values(self, shape):
        """Return uniformly-distributed dates in 2014."""
        start = Timestamp("2014", tz="UTC").asm8
        offsets = self.state.randint(
            low=0,
            high=364,
            size=shape,
        ).astype("timedelta64[D]")
        return start + offsets

    def _bool_values(self, shape):
        """Return uniformly-distributed True/False values."""
        return self.state.randn(*shape) < 0

    def _object_values(self, shape):
        res = self._int_values(shape).astype(str).astype(object)
        return res

