"""Bond arithmetic: turning a quote into money, and a schedule into cash flows.

A bond quote is not a price and a bond position is not ``amount * price``. Three corrections stand
between the tape and the ledger, and all three live here:

#. **Quotation.** Exchanges quote a bond as a percentage of face value, so ``98.42`` on a
   1000-unit nominal is 984.20 per bond.
#. **Accrued interest (НКД).** Between coupons the buyer owes the seller the part of the coupon
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
import datetime
from bisect import bisect_right

import structlog

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent


def _as_date(value) -> datetime.date | None:
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


class BondBook:
    """The coupon and amortization schedules of the bonds a simulation touches.

    The ledger settles transactions synchronously, so schedules are loaded ahead of time -- when a
    bond is first ordered, and once per session for every bond held -- and answered from memory
    afterwards. A bond with no schedule loaded is not an error: accrued interest is then taken
    from its own coupon rate, and its face value is the nominal at issue.
    """

    def __init__(self, logger=None):
        self._logger = logger or structlog.get_logger(__name__)
        #: bond asset id -> events sorted by payment date.
        self._events: dict[int, list[BondEvent]] = {}
        #: bond asset id -> amortization events sorted by date, with their cumulative face values.
        self._amortizations: dict[int, list[BondEvent]] = {}
        #: bond asset id -> coupon events sorted by payment date.
        self._coupons: dict[int, list[BondEvent]] = {}

    # -- loading ------------------------------------------------------------------------------

    def __contains__(self, bond_id: int) -> bool:
        return bond_id in self._events

    def add(self, bond_id: int, events: list[BondEvent]) -> None:
        """Store (replacing) the schedule of one bond."""
        ordered = sorted(events, key=lambda event: (event.date, event.event_type.value))
        self._events[bond_id] = ordered
        self._coupons[bond_id] = [e for e in ordered
                                  if e.event_type in (BondEventType.COUPON, BondEventType.DIVIDEND)]
        self._amortizations[bond_id] = [e for e in ordered
                                        if e.event_type is BondEventType.AMORTIZATION]

    async def load(self, asset_service, bonds) -> None:
        """Fetch and cache the schedules of ``bonds`` that are not loaded yet.

        Silently does nothing for a service that cannot serve bond events, so a simulation driven
        by a stub asset service still runs -- it simply prices bonds clean.
        """
        missing = []
        seen = set()
        for bond in bonds:
            if bond is None or not isinstance(bond, Bond):
                continue
            if bond.id in self._events or bond.id in seen:
                continue
            seen.add(bond.id)
            missing.append(bond)
        if not missing:
            return
        getter = getattr(asset_service, "get_bond_events", None)
        events_by_bond = {} if getter is None else await getter(bonds=missing)
        for bond in missing:
            events = events_by_bond.get(bond.id) or []
            if events:
                self.add(bond.id, events)
                continue
            # A bond nobody ingested a schedule for still has terms of its own; using them beats
            # pricing it as if it paid nothing. Warned about rather than done quietly, because the
            # generated coupons are an inference and the ingested ones are not -- and for a
            # zero-coupon bond the empty schedule is the truth, so nothing is generated at all.
            generated = coupon_schedule(bond)
            if generated:
                self._logger.warning(
                    "No stored schedule for a coupon-bearing bond; generating one from its own "
                    "terms. Ingest its calendar to use the issuer's actual payments.",
                    bond=bond.asset_name, coupon_rate=bond.coupon_rate,
                    coupon_frequency=bond.coupon_frequency, coupons=len(generated))
            self.add(bond.id, generated)

    def events(self, bond: Bond) -> list[BondEvent]:
        """Every stored event of a bond, ordered by payment date."""
        return self._events.get(bond.id, [])

    # -- valuation ----------------------------------------------------------------------------

    def face_value(self, bond: Bond, dt) -> float:
        """Principal outstanding on ``dt``, after every instalment repaid up to and including it.

        Computed as the nominal at issue minus everything repaid, rather than read from an
        event's ``new_face_value``. That field looks authoritative and is not: vendors have been
        observed reporting *today's* outstanding nominal on every instalment -- every future
        amortization of an issue carrying the same number -- so trusting it would freeze the face
        value for the rest of the bond's life. Instalments sum to the nominal, which makes the
        subtraction exact.

        After maturity the outstanding principal is zero: the issuer has repaid it.
        """
        as_of = _as_date(dt)
        if as_of is not None and bond.maturity_date is not None and as_of > bond.maturity_date:
            return 0.0
        amortizations = self._amortizations.get(bond.id)
        if not amortizations or as_of is None:
            return bond.face_value
        repaid = sum(event.value for event in amortizations if event.date <= as_of)
        return max(0.0, bond.face_value - repaid)

    def accrued_interest(self, bond: Bond, dt) -> float:
        """Coupon accrued on one bond by ``dt`` (НКД), in the bond's currency.

        Zero on a coupon payment date -- the coupon has just been paid and a new period begins --
        and zero for a zero-coupon bond, which accrues nothing to hand to a seller.
        """
        as_of = _as_date(dt)
        if as_of is None:
            return 0.0
        if bond.maturity_date is not None and as_of >= bond.maturity_date:
            return 0.0

        coupons = self._coupons.get(bond.id)
        if coupons:
            return self._accrued_from_schedule(coupons, as_of)
        return self._accrued_from_rate(bond, as_of)

    @staticmethod
    def _accrued_from_schedule(coupons: list[BondEvent], as_of: datetime.date) -> float:
        """Accrual read straight off the schedule.

        Within a coupon period the accrual is linear in elapsed days -- exactly how an exchange
        publishes НКД -- so no day-count convention is involved once the period is known.
        """
        dates = [coupon.date for coupon in coupons]
        index = bisect_right(dates, as_of)
        if index >= len(coupons):
            # Past the last coupon: nothing left to accrue.
            return 0.0
        coupon = coupons[index]
        period_start = coupon.period_start_date
        if period_start is None:
            period_start = coupons[index - 1].date if index else None
        if period_start is None or period_start > as_of:
            # The bond has not entered this coupon period yet (it was issued after ``as_of``, or
            # the schedule starts later than the date asked about).
            return 0.0
        period_days = (coupon.date - period_start).days
        if period_days <= 0:
            return 0.0
        elapsed = (as_of - period_start).days
        return coupon.value * elapsed / period_days

    @staticmethod
    def _accrued_from_rate(bond: Bond, as_of: datetime.date) -> float:
        """Accrual from the bond's own terms, for a bond whose schedule was never loaded."""
        if bond.is_zero_coupon:
            return 0.0
        schedule = coupon_schedule(bond)
        if schedule:
            return BondBook._accrued_from_schedule(schedule, as_of)
        # No schedule could be derived either: accrue from the start of the bond's life.
        start = bond.first_traded or bond.start_date
        if start is None or start > as_of:
            return 0.0
        return bond.annual_coupon_amount * bond.day_count.year_fraction(start, as_of)

    @staticmethod
    def valuation_date(bond: Bond, dt):
        """The date a quote should be valued against, never later than maturity.

        :meth:`face_value` reports nothing outstanding after maturity, which is the truth about the
        issuer's remaining obligation but the wrong basis for valuing the redemption itself -- that
        trade settles *at* maturity, on a session that may fall days later because maturity landed
        on a weekend. Valuing it at the session date would repay a percentage of nothing.
        """
        as_of = _as_date(dt)
        if as_of is not None and bond.maturity_date is not None and as_of > bond.maturity_date:
            return bond.maturity_date
        return as_of

    def clean_value(self, bond: Bond, quoted_price: float, dt) -> float:
        """Money one bond is worth at ``quoted_price``, excluding accrued interest."""
        as_of = self.valuation_date(bond, dt)
        return bond.price_quotation.money_price(quoted_price, self.face_value(bond, as_of))

    def dirty_value(self, bond: Bond, quoted_price: float, dt) -> float:
        """Money one bond changes hands for: clean value plus accrued interest."""
        return self.clean_value(bond, quoted_price, dt) + self.accrued_interest(bond, dt)

    def money_per_quote_unit(self, bond: Bond, dt) -> float:
        """Money a one-point move in the quote is worth, for one bond.

        The bond analogue of a futures multiplier: it converts a commission or a price difference
        expressed in money into the quote units a cost basis is carried in.
        """
        if bond.price_quotation is PriceQuotation.MONEY:
            return 1.0
        return self.face_value(bond, self.valuation_date(bond, dt)) / 100.0

    # -- schedule queries ---------------------------------------------------------------------

    def payments_entitled_between(self, bond: Bond, after, through) -> list[BondEvent]:
        """Cash-paying events whose entitlement falls in ``(after, through]``.

        An interval rather than an exact date because a record date is a calendar date and the
        simulation only wakes on trading sessions. A coupon whose record date lands on a Saturday
        would otherwise never be earned by anyone -- which, on a semi-annual bond, silently drops
        about two coupons in seven.
        """
        lower, upper = _as_date(after), _as_date(through)
        return [event for event in self._events.get(bond.id, [])
                if event.pays_cash and lower < event.entitlement_date <= upper]

    def events_on(self, bond: Bond, date) -> list[BondEvent]:
        """Every stored event dated ``date``, cash-paying or not."""
        as_of = _as_date(date)
        return [event for event in self._events.get(bond.id, []) if event.date == as_of]

    def next_offer(self, bond: Bond, date) -> BondEvent | None:
        """The next put/call window at or after ``date``, if the schedule carries one."""
        as_of = _as_date(date)
        for event in self._events.get(bond.id, []):
            if event.event_type is BondEventType.OFFER and event.date >= as_of:
                return event
        return None


def current_yield(bond: Bond, book: BondBook, quoted_price: float, dt) -> float:
    """Annual coupon income as a fraction of what the bond costs to buy today.

    The simplest of the yield measures and the only one that needs no assumption about
    reinvestment: ``annual coupons / dirty price``. Zero for a bond that pays no coupons.
    """
    dirty = book.dirty_value(bond, quoted_price, dt)
    if dirty <= 0 or bond.is_zero_coupon:
        return 0.0
    return bond.annual_coupon_amount / dirty


def simple_yield_to_maturity(bond: Bond, book: BondBook, quoted_price: float, dt) -> float:
    """Annualised return of buying at ``quoted_price`` and holding to maturity, without compounding.

    ``(remaining coupons + redemption - price paid) / price paid``, annualised over the years left.
    A simple approximation rather than an IRR: it is the number a rouble-bond desk quotes as
    "простая доходность", and it needs no solver, which keeps it usable inside a bar loop.
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
