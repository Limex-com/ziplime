"""Date utils for bar_data."""

import datetime


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    """The calendar day of a listing bound, which may be stored either way."""
    return value.date() if isinstance(value, datetime.datetime) else value
