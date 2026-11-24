"""Rate utils for market."""

import datetime
import pandas as pd
from ziplime.sweep.params import SweepError


def parse_rate(value) -> datetime.timedelta:
    """An emission rate or history frequency: a timedelta, or "1m", "5m", "1h", "1d", "1min"..."""
    if value is None:
        return datetime.timedelta(days=1)
    if isinstance(value, datetime.timedelta):
        return value
    text = str(getattr(value, "value", value)).strip().lower()
    for suffix, unit in (("min", "minutes"), ("m", "minutes"), ("h", "hours"), ("d", "days")):
        if text.endswith(suffix) and text[:-len(suffix)].strip().isdigit():
            return datetime.timedelta(**{unit: int(text[:-len(suffix)])})
    try:
        return pd.Timedelta(text).to_pytimedelta()
    except ValueError:
        raise SweepError(f"Not a bar frequency: {value!r} (use 1m, 5m, 1h, 1d).") from None
