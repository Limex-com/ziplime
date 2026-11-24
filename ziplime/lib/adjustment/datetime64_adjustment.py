"""Datetime64 adjustment for adjustment."""

from pandas import Timestamp
from numpy import datetime64
from ziplime.lib.adjustment.int64_adjustment import _Int64Adjustment
from ziplime.lib.adjustment.datetime_utils import datetime_to_int


class Datetime64Adjustment(_Int64Adjustment):
    """
    Base class for adjustments that operate on Datetime64 data.

    Notes
    -----
    Numpy stores datetime64 values in arrays of type int64.  There's no
    straightforward way to work with statically-typed datetime64 data, so
    instead we work with int64 values everywhere, and we do validation/coercion
    at API boundaries.
    """

    def __init__(self,
                 first_row: int,
                 last_row: int,
                 first_col: int,
                 last_col: int,
                 value: datetime64 | Timestamp) -> None:
        super(Datetime64Adjustment, self).__init__(
            first_row=first_row,
            last_row=last_row,
            first_col=first_col,
            last_col=last_col,
            value=datetime_to_int(value),
        )

    def __repr__(self):
        return (
                "%s(first_row=%d, last_row=%d,"
                " first_col=%d, last_col=%d, value=%r)" % (
                    type(self).__name__,
                    self.first_row,
                    self.last_row,
                    self.first_col,
                    self.last_col,
                    datetime64(self.value, 'ns'),
                )
        )

    def __reduce__(self):
        return type(self), (
            self.first_row,
            self.last_row,
            self.first_col,
            self.last_col,
            datetime64(self.value, 'ns'),
        )
