"""Bond arithmetic: turning a quote into money, and a schedule into cash flows.

A bond quote is not a price and a bond position is not ``amount * price``. Three corrections stand
between the tape and the ledger, and all three live here:

#. **Quotation.** Exchanges quote a bond as a percentage of face value, so ``98.42`` on a
   1000-unit nominal is 984.20 per bond.
#. **Accrued interest.** Between coupons the buyer owes the seller the part of the coupon
   that has already accrued. The tape carries the *clean* price; settlement happens at the *dirty*
   one. Over a year of round trips this is not a rounding error -- on an 8% coupon it averages
   4% of face value per trade.
#. **Amortization.** An amortizing issue repays principal in instalments, so the face value a
   quote is a percentage *of* shrinks over the bond's life.

:class:`BondBook` holds the schedules the simulation has loaded and answers all three questions
synchronously, because the ledger settles a transaction inside a synchronous call and cannot go
back to the database at that point.

Accrual is computed from the schedule whenever one is present -- elapsed days over the length of
the coupon period, which is what an exchange actually publishes -- and falls back to the bond's
own annual rate and day-count convention only when no schedule was loaded.
"""

from ziplime.finance.bonds.schedule_utils import coupon_schedule  # noqa: F401
from ziplime.finance.bonds.bond_book import BondBook  # noqa: F401
from ziplime.finance.bonds.utils import current_yield  # noqa: F401
from ziplime.finance.bonds.utils import simple_yield_to_maturity  # noqa: F401


__all__ = [
    'coupon_schedule',
    'BondBook',
    'current_yield',
    'simple_yield_to_maturity',
]
