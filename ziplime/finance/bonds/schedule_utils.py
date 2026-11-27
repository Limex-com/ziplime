"""Schedule utils for bonds."""

import datetime
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date | None:
    """Normalise a date-or-datetime to a date. Callers pass whatever the simulation clock holds."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    return value


def coupon_schedule(bond: Bond, first_coupon_date: datetime.date | None = None) -> list[BondEvent]:
    """Generate the coupon schedule implied by a bond's own terms.

    Used to fill in a bond whose schedule was never ingested, and by the demo universe to build a
    consistent one. Coupons are laid out backwards from ``maturity_date`` at the stated frequency,
    which is how issuers actually date them: the final coupon lands on maturity and any short
    period falls at the start of the bond's life, not the end.

    Returns an empty list for a zero-coupon bond, which is the correct schedule for one.
    """
    if bond.is_zero_coupon or bond.maturity_date is None:
        return []

    months = max(1, round(12 / bond.coupon_frequency))
    amount = bond.coupon_amount()
    start_bound = first_coupon_date or bond.start_date or bond.first_traded
    # Every date is measured from maturity rather than from the previous one: stepping back a
    # month at a time drifts whenever a period end has to be clamped to a shorter month.
    dates: list[datetime.date] = []
    for step in range(2000):
        date = _subtract_months(bond.maturity_date, months * step)
        dates.append(date)
        if start_bound is not None and _subtract_months(bond.maturity_date,
                                                        months * (step + 1)) < start_bound:
            break
    dates.reverse()

    events = []
    first_period_start = _subtract_months(bond.maturity_date, months * len(dates))
    for index, pay_date in enumerate(dates):
        period_start = first_period_start if index == 0 else dates[index - 1]
        events.append(BondEvent(
            asset=bond,
            event_type=BondEventType.COUPON,
            date=pay_date,
            value=amount,
            currency=bond.quote_currency,
            record_date=None,
            period_start_date=period_start,
            face_value=bond.face_value,
            value_percent=bond.coupon_rate * 100.0,
        ))
    return events


def _subtract_months(date: datetime.date, months: int) -> datetime.date:
    """Step back whole months, clamping the day to the length of the target month."""
    month_index = date.month - 1 - months
    year = date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(date.day, _days_in_month(year, month))
    return datetime.date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (datetime.date(year, month + 1, 1) - datetime.date(year, month, 1)).days
