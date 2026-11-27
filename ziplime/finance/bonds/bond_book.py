"""Bond book for bonds."""

import datetime
from bisect import bisect_right
from collections.abc import Iterable
import structlog
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.services.asset_service import AssetService
from ziplime.finance.bonds.schedule_utils import _as_date
from ziplime.finance.bonds.schedule_utils import coupon_schedule


class BondBook:
    """The coupon and amortization schedules of the bonds a simulation touches.

    The ledger settles transactions synchronously, so schedules are loaded ahead of time -- when a
    bond is first ordered, and once per session for every bond held -- and answered from memory
    afterwards. A bond with no schedule loaded is not an error: accrued interest is then taken
    from its own coupon rate, and its face value is the nominal at issue.
    """

    def __init__(self, logger=None) -> None:
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

    async def load(self, asset_service: AssetService, bonds: Iterable[Bond]) -> None:
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

    def face_value(self, bond: Bond, dt: datetime.date | datetime.datetime) -> float:
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

    def accrued_interest(self, bond: Bond, dt: datetime.date | datetime.datetime) -> float:
        """Coupon accrued on one bond by ``dt``, in the bond's currency.

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
        publishes accrued interest directly -- so no day-count convention is involved once the
    period is known.
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
    def valuation_date(bond: Bond, dt: datetime.date | datetime.datetime):
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

    def clean_value(self, bond: Bond, quoted_price: float, dt: datetime.date | datetime.datetime) -> float:
        """Money one bond is worth at ``quoted_price``, excluding accrued interest."""
        as_of = self.valuation_date(bond, dt)
        return bond.price_quotation.money_price(quoted_price, self.face_value(bond, as_of))

    def dirty_value(self, bond: Bond, quoted_price: float, dt: datetime.date | datetime.datetime) -> float:
        """Money one bond changes hands for: clean value plus accrued interest."""
        return self.clean_value(bond, quoted_price, dt) + self.accrued_interest(bond, dt)

    def money_per_quote_unit(self, bond: Bond, dt: datetime.date | datetime.datetime) -> float:
        """Money a one-point move in the quote is worth, for one bond.

        The bond analogue of a futures multiplier: it converts a commission or a price difference
        expressed in money into the quote units a cost basis is carried in.
        """
        if bond.price_quotation is PriceQuotation.MONEY:
            return 1.0
        return self.face_value(bond, self.valuation_date(bond, dt)) / 100.0

    # -- schedule queries ---------------------------------------------------------------------

    def payments_entitled_between(
        self,
        bond: Bond,
        after: datetime.date | datetime.datetime,
        through: datetime.date | datetime.datetime,
    ) -> list[BondEvent]:
        """Cash-paying events whose entitlement falls in ``(after, through]``.

        An interval rather than an exact date because a record date is a calendar date and the
        simulation only wakes on trading sessions. A coupon whose record date lands on a Saturday
        would otherwise never be earned by anyone -- which, on a semi-annual bond, silently drops
        about two coupons in seven.
        """
        lower, upper = _as_date(after), _as_date(through)
        return [event for event in self._events.get(bond.id, [])
                if event.pays_cash and lower < event.entitlement_date <= upper]

    def events_on(self, bond: Bond, date: datetime.date | datetime.datetime) -> list[BondEvent]:
        """Every stored event dated ``date``, cash-paying or not."""
        as_of = _as_date(date)
        return [event for event in self._events.get(bond.id, []) if event.date == as_of]

    def next_offer(self, bond: Bond, date: datetime.date | datetime.datetime) -> BondEvent | None:
        """The next put/call window at or after ``date``, if the schedule carries one."""
        as_of = _as_date(date)
        for event in self._events.get(bond.id, []):
            if event.event_type is BondEventType.OFFER and event.date >= as_of:
                return event
        return None
