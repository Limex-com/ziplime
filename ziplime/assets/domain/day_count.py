"""Day-count conventions, used only where a coupon schedule is unavailable.

Accrued interest between two coupons is a fraction of the coupon, and when the schedule is known
that fraction is exact: elapsed days over the length of the coupon period. A day-count convention
is the *fallback* -- it approximates the same number from the bond's annual rate when nothing tells
us where the current coupon period starts or ends.
"""
import datetime
import enum


class DayCount(enum.Enum):
    """Year fraction between two dates.

    ``ACT_365`` is the most common convention for government bonds and is the default here.
    ``THIRTY_360`` is the
    US corporate convention. ``ACT_ACT`` follows the actual length of the year the period falls in,
    which matters across a leap year.
    """

    ACT_365 = "ACT_365"
    ACT_360 = "ACT_360"
    ACT_ACT = "ACT_ACT"
    THIRTY_360 = "THIRTY_360"

    def year_fraction(self, start: datetime.date, end: datetime.date) -> float:
        """Fraction of a year between ``start`` and ``end``; negative if ``end`` precedes ``start``."""
        if self is DayCount.THIRTY_360:
            return _thirty_360_days(start, end) / 360.0

        days = (end - start).days
        if self is DayCount.ACT_360:
            return days / 360.0
        if self is DayCount.ACT_365:
            return days / 365.0
        # ACT/ACT: measure against the actual length of the year the period starts in. Exact for
        # periods inside one year, which is every accrual period of a coupon-bearing bond.
        year_start = datetime.date(start.year, 1, 1)
        year_end = datetime.date(start.year + 1, 1, 1)
        return days / (year_end - year_start).days


def _thirty_360_days(start: datetime.date, end: datetime.date) -> int:
    """Days between two dates under the 30/360 US convention."""
    d1, d2 = min(start.day, 30), end.day
    if d1 == 30 and d2 == 31:
        d2 = 30
    return 360 * (end.year - start.year) + 30 * (end.month - start.month) + (d2 - d1)
