"""Date calculations for bond day-count conventions."""

import datetime


def _thirty_360_days(start: datetime.date, end: datetime.date) -> int:
    """Days between two dates under the 30/360 US convention."""
    d1, d2 = min(start.day, 30), end.day
    if d1 == 30 and d2 == 31:
        d2 = 30
    return 360 * (end.year - start.year) + 30 * (end.month - start.month) + (d2 - d1)
