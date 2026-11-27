"""Date utils for signals."""

import datetime
import pandas as pd


def _as_stamp(now: datetime.datetime, index: pd.DatetimeIndex) -> pd.Timestamp:
    """The simulation time, made comparable with the panel's index."""
    stamp = pd.Timestamp(now)
    if index.tz is None:
        return stamp.tz_localize(None) if stamp.tzinfo is not None else stamp
    return stamp.tz_localize(index.tz) if stamp.tzinfo is None else stamp.tz_convert(index.tz)
