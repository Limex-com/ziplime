"""Turning Finam's bond endpoints into ziplime bonds and schedules.

Three endpoints carry everything ziplime needs, and none carries all of it alone:

* ``GET /v1/assets/all`` enumerates every instrument, archived ones included, with a ``type`` of
  ``BONDS`` and little else -- ticker, MIC, ISIN, name. No nominal, no coupon, no maturity.
* ``GET /v1/bonds/past`` returns the calendar the bond has already paid.
* ``GET /v1/bonds/future`` returns the calendar it has not reached yet, ending in the amortization
  that repays the last of the principal.

So the **terms are read off the calendar**, and :func:`infer_bond_terms` records in
:attr:`BondTerms.inferred` which fields were derived rather than stated.

Four things about the payloads are not obvious and each one silently corrupts a bond if missed.
All were established against the live API rather than from the documentation:

**Redemption arrives as an amortization, not as a maturity event.** ``MATURITY`` never appears. An
OFZ ends with ``AMORTIZATION`` of 100% of the nominal on its maturity date; an amortizing corporate
issue ends with an instalment that happens to repay the remainder. Across a sample of forty issues
the amortizations always summed to exactly the nominal at issue. :func:`normalize_events` therefore
re-types the instalment that repays the last of the principal as
:attr:`~ziplime.assets.domain.bond_event_type.BondEventType.MATURITY`, so redemption is booked once
-- by the ledger, at par against what is outstanding -- rather than twice.

**``new_face_value`` is today's outstanding nominal, not the nominal after that instalment.** Every
future amortization of ``СЗКК 03`` reports ``new_face_value`` of 567.0 -- the same figure on all
eleven, which is what is outstanding now. Reading it as "the nominal this instalment leaves behind"
would freeze the face value for the rest of the bond's life. The outstanding amount is instead
computed as ``initial_face_value`` minus everything repaid so far, which the sums above make exact.

**``coupon_details.face_value`` is today's outstanding nominal too**, not the basis the coupon was
computed on -- so the nominal at issue is taken from ``amortization_details.initial_face_value``,
and only falls back to the coupon's figure for a bond with no amortization at all, where the two
coincide.

**A coupon of zero means "not set yet", not "pays nothing".** Floating-rate issues report future
coupons with ``value`` and ``value_percent`` both zero because the rate has not been fixed. Those
are counted and reported rather than taken at face value.

One limitation worth knowing before planning a study: **archived bonds have no calendar**. Every
redeemed issue sampled returned nothing from both endpoints, so a bond that matured before today
cannot be reconstructed from this API. Backtests are limited to issues that are still listed --
their *history* is fully available, which is what matters, but a survivorship-free universe of
redeemed bonds is not obtainable here.
"""
import dataclasses
import datetime
from dataclasses import dataclass
from typing import Any

import structlog

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.data.data_sources.finam.finam_client import parse_date, parse_decimal

_logger = structlog.get_logger(__name__)

#: Finam's ``type`` for a debt security in ``AllAssets``. Plural, and not ``BOND``.
BOND_ASSET_TYPE = "BONDS"

#: MOEX main market, where rouble bonds trade.
MISX_MIC = "MISX"

#: Coupon frequencies an issuer actually uses, in payments per year. The spacing between observed
#: payments is snapped to the nearest of these rather than taken literally, because a holiday
#: shifts a payment by a few days and 366/91 is not a frequency anyone issues at.
KNOWN_FREQUENCIES = (1, 2, 4, 12)

#: Nominal assumed when the calendar never states one. Standard for rouble issues; a bond whose
#: nominal is anything else and whose coupons carry no face value cannot be priced correctly, so
#: this is logged when it is used.
DEFAULT_FACE_VALUE = 1000.0

#: Finam reports the currency as a symbol rather than an ISO code.
CURRENCY_SYMBOLS = {"\u20bd": "RUB", "$": "USD", "\u20ac": "EUR", "\u00a5": "CNY",
                    "\u00a3": "GBP", "\u20b8": "KZT"}

#: Share of the nominal the instalments must add up to before the schedule is believed to cover
#: the whole principal. Deliberately loose: instalments are published to the kopeck and a long
#: schedule accumulates error in both directions -- ``RUS-30`` repays 1.08 of a 1.00 nominal in
#: one-kopeck steps -- so this is a completeness check, not an equality.
REPAYMENT_COMPLETENESS = 0.95


def normalize_currency(value: str | None, default: str = "RUB") -> str:
    """Map Finam's currency symbol onto an ISO code, passing an existing code through."""
    if not value:
        return default
    text = str(value).strip()
    if text in CURRENCY_SYMBOLS:
        return CURRENCY_SYMBOLS[text]
    # Already an ISO code, or something we have not seen; upper-case it and move on.
    return text.upper() if len(text) == 3 and text.isalpha() else default


@dataclass(frozen=True)
class BondTerms:
    """Terms read off a bond's realised calendar.

    Attributes:
        face_value: Nominal at issue.
        maturity_date: Last dated event, or ``None`` when the calendar does not reach it.
        coupon_rate: Annual coupon as a fraction of face value.
        coupon_frequency: Payments per year; 0 when no coupon was ever observed.
        is_amortized: Whether principal is repaid in instalments before maturity.
        undetermined_coupons: Scheduled coupons whose rate the vendor has not fixed yet -- a
            floating-rate issue reports them as zero. Counted rather than believed.
        inferred: Names of the fields that were derived rather than stated by the vendor.
    """

    face_value: float
    maturity_date: datetime.date | None
    coupon_rate: float
    coupon_frequency: int
    is_amortized: bool
    undetermined_coupons: int = 0
    inferred: tuple[str, ...] = ()


def is_bond_listing(listing: dict[str, Any]) -> bool:
    """Whether an ``AllAssets`` row describes a bond."""
    return (listing.get("type") or "").upper() == BOND_ASSET_TYPE


def parse_bond_event(payload: dict[str, Any], bond: Bond) -> BondEvent | None:
    """Build a :class:`BondEvent` from one ``GetPastBondsEvents`` entry.

    Returns ``None`` for an entry with no usable date -- there is nothing a dateless event can
    contribute to a schedule, and dropping it beats inventing a date for it.
    """
    date = parse_date(payload.get("date"))
    if date is None:
        return None

    event_type = BondEventType.from_api(payload.get("type"))
    coupon = payload.get("coupon_details") or {}
    amortization = payload.get("amortization_details") or {}
    offer = payload.get("offer_details") or {}

    return BondEvent(
        asset=bond,
        event_type=event_type,
        date=date,
        value=parse_decimal(payload.get("value")) or 0.0,
        currency=normalize_currency(payload.get("currency"), default=bond.quote_currency),
        record_date=parse_date(coupon.get("record_date")),
        period_start_date=parse_date(coupon.get("start_date")),
        face_value=parse_decimal(coupon.get("face_value")),
        value_percent=parse_decimal(coupon.get("value_percent")),
        new_face_value=parse_decimal(amortization.get("new_face_value")),
        initial_face_value=parse_decimal(amortization.get("initial_face_value")),
        amortization_percent=parse_decimal(amortization.get("amortization_percent")),
        offer_type=offer.get("offer_type") or None,
        offer_price=parse_decimal(offer.get("price")),
        offer_start_date=parse_date(offer.get("start_date")),
        offer_end_date=parse_date(offer.get("end_date")),
        offer_agent=offer.get("agent") or None,
    )


def parse_bond_events(payloads: list[dict[str, Any]], bond: Bond) -> list[BondEvent]:
    """Parse a whole ``GetPastBondsEvents`` list, dropping entries with no date."""
    parsed = (parse_bond_event(payload, bond) for payload in payloads or [])
    return sorted((event for event in parsed if event is not None), key=lambda e: e.date)


def initial_face_value(events: list[BondEvent]) -> float | None:
    """The nominal at issue.

    Taken from ``amortization_details.initial_face_value``, which is the only field that reports it.
    ``coupon_details.face_value`` looks like the same thing and is not -- it carries what is
    outstanding *today* -- so it is used only when the issue has no amortization at all, where the
    two necessarily agree.
    """
    for event in events:
        if event.event_type is BondEventType.AMORTIZATION and event.initial_face_value:
            return event.initial_face_value
    if any(event.event_type is BondEventType.AMORTIZATION for event in events):
        # Amortizing, but the vendor never stated the nominal at issue; the coupon's figure is
        # today's outstanding and would understate it.
        return None
    for event in events:
        if event.event_type is BondEventType.COUPON and event.face_value:
            return event.face_value
    return None


def normalize_events(events: list[BondEvent], face_value: float,
                     logger=None) -> list[BondEvent]:
    """Re-type the instalment that repays the last of the principal as a redemption.

    Finam never sends ``MATURITY``: an OFZ ends with a single ``AMORTIZATION`` of 100% of its
    nominal, and an amortizing issue ends with the instalment that clears the remainder. Left
    alone, that instalment would be paid as a coupon-like cash flow *and* the position would then
    be redeemed at par by the ledger -- the principal counted twice.

    The redemption is identified as the **last** instalment in the schedule rather than the one at
    which the running total first reaches the nominal. Running totals drift: ``RUS-30`` repays a
    nominal of 1.00 in 47 one-kopeck steps that sum to 1.08, so a cumulative threshold declares it
    redeemed in 2025 when it actually matures in 2030. The schedule is instead checked as a whole
    -- if the instalments together account for the principal, the last of them is the redemption.

    A calendar whose instalments fall well short of the nominal is left untouched: that is a
    partly published schedule rather than a bond that repays a fraction of itself.
    """
    logger = logger or _logger
    ordered = sorted(events, key=lambda event: (event.date, event.event_type.value))
    amortizations = [event for event in ordered
                     if event.event_type is BondEventType.AMORTIZATION]
    if not amortizations:
        return ordered

    repaid = sum(event.value for event in amortizations)
    if face_value and repaid < face_value * REPAYMENT_COMPLETENESS:
        logger.warning(
            "A bond's instalments do not account for its principal; leaving the schedule alone. "
            "Its redemption is most likely not published yet.",
            face_value=face_value, repaid=round(repaid, 4), instalments=len(amortizations))
        return ordered

    terminal = amortizations[-1]
    if face_value and repaid > face_value * (2 - REPAYMENT_COMPLETENESS):
        # The vendor's own instalments do not reconcile with the nominal it reports. Said out loud
        # because it means the outstanding principal this bond is valued against carries that error.
        logger.warning(
            "A bond's instalments overshoot its nominal; the outstanding principal derived from "
            "them will be off by the difference.",
            face_value=face_value, repaid=round(repaid, 4), instalments=len(amortizations))

    return [dataclasses.replace(event, event_type=BondEventType.MATURITY)
            if event is terminal else event
            for event in ordered]


def infer_bond_terms(events: list[BondEvent],
                     details: dict[str, Any] | None = None) -> BondTerms:
    """Read a bond's terms off its realised calendar, and off ``GetAsset`` where it answers.

    ``details`` -- the ``GetAsset`` payload -- wins wherever it states something, because it is the
    issuer's own specification. Everything it leaves out is derived from the calendar, and every
    derived field is named in :attr:`BondTerms.inferred` so a caller can tell the two apart.
    """
    details = details or {}
    inferred: list[str] = []

    coupons = [event for event in events if event.event_type is BondEventType.COUPON]
    amortizations = [event for event in events if event.event_type is BondEventType.AMORTIZATION]

    # The calendar wins: only ``amortization_details.initial_face_value`` reports the nominal at
    # issue. ``GetAsset`` reports what is outstanding, which coincides with it only for an issue
    # that has never repaid principal.
    face_value = initial_face_value(events)
    if face_value is not None:
        inferred.append("face_value")
    else:
        face_value = None if amortizations else outstanding_face_value(details)
    if face_value is None:
        face_value = DEFAULT_FACE_VALUE
        inferred.append("face_value")

    maturity_date = (parse_date(details.get("maturity_date"))
                     or parse_date(details.get("expiration_date")))
    if maturity_date is None:
        maturity_date = _maturity_from_events(events)
        if maturity_date is not None:
            inferred.append("maturity_date")

    frequency = _frequency_from_coupons(coupons)
    if frequency:
        inferred.append("coupon_frequency")

    rate = _rate_from_coupons(coupons, face_value=face_value, frequency=frequency)
    if rate:
        inferred.append("coupon_rate")

    return BondTerms(
        face_value=face_value,
        maturity_date=maturity_date,
        coupon_rate=rate,
        coupon_frequency=frequency,
        # A single instalment repaying the whole nominal is a bullet redemption, not amortization.
        is_amortized=len(amortizations) > 1,
        undetermined_coupons=sum(1 for coupon in coupons
                                 if not coupon.value and not coupon.value_percent),
        inferred=tuple(inferred),
    )


def outstanding_face_value(details: dict[str, Any]) -> float | None:
    """Nominal still outstanding, as ``GetAsset`` reports it in ``bond_details.bond_face_value``.

    Outstanding, not the nominal at issue: ``RUS-30`` reports 0.04 against a nominal of 1.00,
    because most of its principal has already been amortized. Safe as the nominal only for an issue
    that has never repaid any principal.

    ``lot_size`` is deliberately **not** consulted. It is the trading lot and looks plausible
    enough to be mistaken for the nominal -- it is 1.0 for an OFZ whose nominal is 1000, and 1000.0
    for ``RUS-30`` whose nominal is 1.0, so reading it gets both wrong and in opposite directions.
    """
    bond_details = details.get("bond_details") or {}
    value = parse_decimal(bond_details.get("bond_face_value"))
    if value:
        return value
    for key in ("face_value", "nominal", "par_value"):
        value = parse_decimal(details.get(key))
        if value:
            return value
    return None


def _maturity_from_events(events: list[BondEvent]) -> datetime.date | None:
    """Maturity: the redemption event if the calendar reaches one, otherwise its last dated event.

    ``normalize_events`` has already marked the instalment that clears the principal, so the first
    branch is the normal case for a live bond and the fallback covers a calendar that stops short
    -- which is what a bond whose schedule is only partly published looks like.
    """
    redemptions = [event.date for event in events
                   if event.event_type is BondEventType.MATURITY]
    if redemptions:
        return max(redemptions)
    return max((event.date for event in events), default=None)


def _frequency_from_coupons(coupons: list[BondEvent]) -> int:
    """Payments per year, from the typical spacing between observed coupons.

    The median gap is used rather than the mean: one shifted payment, or a short first period,
    would otherwise drag the estimate off the frequency the issuer actually pays at.
    """
    dates = sorted({coupon.date for coupon in coupons})
    if len(dates) < 2:
        return 0
    gaps = sorted((later - earlier).days for earlier, later in zip(dates, dates[1:]))
    median_gap = gaps[len(gaps) // 2]
    if median_gap <= 0:
        return 0
    raw = 365.0 / median_gap
    return min(KNOWN_FREQUENCIES, key=lambda candidate: abs(candidate - raw))


def _rate_from_coupons(coupons: list[BondEvent], face_value: float, frequency: int) -> float:
    """Annual coupon as a fraction of face value: the most recently *fixed* rate.

    Reads backwards to the last coupon that carries a rate at all. A floater publishes its future
    coupons with a rate of zero because it has not been set, so taking the chronologically last
    coupon would report every floating-rate bond as paying nothing.

    Prefers the vendor's own ``value_percent``, which is already annualised, and falls back to the
    money paid: one coupon times the frequency, over the nominal it was computed on.
    """
    for coupon in reversed(coupons):
        if coupon.value_percent:
            return coupon.value_percent / 100.0
        if coupon.value:
            basis = coupon.face_value or face_value
            if basis and frequency:
                return coupon.value * frequency / basis
    return 0.0


def build_bond(listing: dict[str, Any], events: list[dict[str, Any]] | None = None,
               details: dict[str, Any] | None = None,
               quote_currency: str | None = None,
               day_count: DayCount = DayCount.ACT_365,
               logger=None) -> tuple[Bond, list[BondEvent]] | None:
    """Assemble a :class:`Bond` and its schedule from the endpoints' raw payloads.

    ``events`` should be the **whole** calendar -- ``past`` and ``future`` concatenated, which is
    what :meth:`FinamClient.bond_events` returns. Either half alone gives a bond that is missing
    either its history or its redemption.

    Returns ``None`` for a listing with no calendar at all, which is what every archived issue
    looks like, and for one whose maturity cannot be established.
    """
    logger = logger or _logger
    ticker = listing.get("ticker") or listing.get("symbol") or ""
    name = listing.get("name") or ticker

    if not events:
        logger.debug("Skipping a bond with no calendar", ticker=ticker, name=name)
        return None

    # Parsed three times against progressively better information: once to read the currency and
    # the nominal, once to mark the redemption, and once against the finished bond so that every
    # event points at the asset it belongs to.
    scratch = Bond(id=None, isin=listing.get("isin") or None, asset_name=ticker,
                   start_date=None, end_date=None, first_traded=None, auto_close_date=None,
                   face_value=DEFAULT_FACE_VALUE, maturity_date=None,
                   quote_currency=quote_currency or "RUB")
    parsed = parse_bond_events(events, scratch)
    currency = quote_currency or _dominant_currency(parsed)

    nominal = initial_face_value(parsed)
    if nominal is None and not any(e.event_type is BondEventType.AMORTIZATION for e in parsed):
        nominal = outstanding_face_value(details or {})
    if nominal is None:
        nominal = DEFAULT_FACE_VALUE
        logger.warning("Assuming the default nominal for a bond whose calendar never states one",
                       ticker=ticker, face_value=nominal)

    normalized = normalize_events(parsed, face_value=nominal)
    terms = infer_bond_terms(normalized, details=details)

    if terms.maturity_date is None:
        logger.warning("Skipping a bond with no maturity date", ticker=ticker, name=name)
        return None
    if terms.undetermined_coupons:
        # A floater: the vendor publishes the dates but not the rates it has not fixed yet.
        logger.warning(
            "Bond has scheduled coupons whose rate is not fixed yet; they will pay nothing. "
            "Most likely a floating-rate issue.",
            ticker=ticker, undetermined=terms.undetermined_coupons)

    first_event = min((event.date for event in normalized), default=None)
    # A coupon period starts before its payment, so the earliest period start is the better
    # estimate of when the bond began its life.
    first_period = min((event.period_start_date for event in normalized
                        if event.period_start_date is not None), default=None)
    start_date = min(d for d in (first_event, first_period, terms.maturity_date) if d is not None)

    bond = Bond(
        id=None,
        isin=listing.get("isin") or None,
        asset_name=ticker,
        start_date=start_date,
        # A bond stops trading before the issuer repays it; redemption books on the maturity date,
        # which is why auto_close sits there rather than a day later.
        end_date=terms.maturity_date - datetime.timedelta(days=1),
        first_traded=start_date,
        auto_close_date=terms.maturity_date,
        face_value=terms.face_value,
        maturity_date=terms.maturity_date,
        coupon_rate=terms.coupon_rate,
        coupon_frequency=terms.coupon_frequency,
        quote_currency=currency,
        day_count=day_count,
        price_quotation=PriceQuotation.PERCENT_OF_FACE,
        is_amortized=terms.is_amortized,
    )
    # Re-bind onto the finished bond, applying the same redemption marking.
    return bond, normalize_events(parse_bond_events(events, bond), face_value=terms.face_value)


def _dominant_currency(events: list[BondEvent]) -> str:
    """The currency most of a bond's payments are made in."""
    counts: dict[str, int] = {}
    for event in events:
        counts[event.currency] = counts.get(event.currency, 0) + 1
    return max(counts, key=counts.get) if counts else "RUB"
