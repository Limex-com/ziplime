"""Utils for bonds."""

import datetime
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.entities.bond import Bond
from ziplime.finance.bonds.bond_book import BondBook
from ziplime.finance.bonds.schedule_utils import _as_date


def current_yield(bond: Bond, book: BondBook, quoted_price: float, dt: datetime.date | datetime.datetime) -> float:
    """Annual coupon income as a fraction of what the bond costs to buy today.

    The simplest of the yield measures and the only one that needs no assumption about
    reinvestment: ``annual coupons / dirty price``. Zero for a bond that pays no coupons.
    """
    dirty = book.dirty_value(bond, quoted_price, dt)
    if dirty <= 0 or bond.is_zero_coupon:
        return 0.0
    return bond.annual_coupon_amount / dirty


def simple_yield_to_maturity(
    bond: Bond,
    book: BondBook,
    quoted_price: float,
    dt: datetime.date | datetime.datetime,
) -> float:
    """Annualised return of buying at ``quoted_price`` and holding to maturity, without compounding.

    ``(remaining coupons + redemption - price paid) / price paid``, annualised over the years left.
    A simple approximation rather than an IRR: it is the number a bond desk quotes as
    "simple yield", and it needs no solver, which keeps it usable inside a bar loop.
    Returns 0.0 for a bond already at or past maturity.
    """
    as_of = _as_date(dt)
    if as_of is None or bond.maturity_date is None or as_of >= bond.maturity_date:
        return 0.0
    years = (bond.maturity_date - as_of).days / 365.0
    paid = book.dirty_value(bond, quoted_price, as_of)
    if paid <= 0 or years <= 0:
        return 0.0
    events = book.events(bond)
    # Coupons and intermediate instalments pay as they fall due...
    future_cash = sum(event.value for event in events if event.pays_cash and event.date > as_of)
    # ...and whatever principal survives them is repaid at maturity. A MATURITY event does not
    # carry cash of its own, so this is the only place the final principal is counted.
    future_amortized = sum(event.value for event in events
                           if event.event_type is BondEventType.AMORTIZATION
                           and event.date > as_of)
    future_cash += max(0.0, book.face_value(bond, as_of) - future_amortized)
    return (future_cash - paid) / paid / years
