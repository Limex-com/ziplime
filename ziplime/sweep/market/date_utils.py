"""Date utils for market."""

import datetime


def _date(value: datetime.date | datetime.datetime):
    return value.date() if isinstance(value, datetime.datetime) else value
