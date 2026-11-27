"""Timezone normalization for command-line parameters."""

import datetime
from zoneinfo import ZoneInfo


def _coerce_tz(tz: str | datetime.tzinfo | None):
    if tz is None or isinstance(tz, datetime.tzinfo):
        return tz
    if isinstance(tz, str):
        return ZoneInfo(tz)
    return tz
