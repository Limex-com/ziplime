"""Date normalization for futures roll selection."""

import datetime


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    return value.date() if isinstance(value, datetime.datetime) else value
