"""Datetime utils for adjustment."""

from pandas import Timestamp
from numpy import datetime64, int64


def datetime_to_int(datetimelike: datetime64 | Timestamp):
    """
    Coerce a datetime-like object to the int format used by AdjustedArrays of
    Datetime64 type.
    """
    if isinstance(datetimelike, Timestamp):
        return datetimelike.value

    if not isinstance(datetimelike, datetime64):
        raise TypeError("Expected datetime64, got %s" % type(datetimelike))

    elif datetimelike.dtype.name != 'datetime64[ns]':
        raise TypeError(
            "Expected datetime64[ns], got %s",
            datetimelike.dtype.name,
        )

    return datetimelike.astype(int64)
