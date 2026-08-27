"""A small offline bond universe, for the examples and the end-to-end tests.

Everything here is **synthetic and clearly labelled as such**: tickers start with ``ZL`` and the
identifiers are ``DEMOBOND...`` rather than ISINs, so nothing in it can be mistaken for a real
MOEX security. It exists because bond behaviour is only visible over years -- a coupon every six
months, an amortization instalment every year, a redemption at the end -- and a test or an example
that needs a whole bond life cannot wait on a vendor, a token, or a network.

The five issues are chosen to cover the cases that behave differently, not to look like a
portfolio:

============== ================================================================================
``ZLB26``      Plain fixed-coupon issue, semi-annual. The paying case.
``ZLZ26``      Zero-coupon discount issue. The *non*-paying control: same dates, no cash flows.
``ZLA27``      Amortizing corporate issue, quarterly coupons, principal repaid in instalments.
``ZLO27``      Fixed-coupon issue with a put window (оферта) partway through its life.
``ZLS25``      Short-dated high-coupon issue that matures inside the example window.
============== ================================================================================

For real instruments, run ``examples/ingest_assets_data_finam_bonds.py`` with a Finam token: it
writes real MOEX issues into the same database alongside these, and the demo bonds can then be
dropped or simply ignored.

Prices are generated, too. :func:`build_demo_bond_bars` produces a clean-price path that pulls to
par as maturity approaches, with a deterministic wobble around it -- enough for a strategy to have
something to react to, and reproducible so a test can assert on the result.
"""
import datetime
import math
import zlib
from dataclasses import dataclass

import polars as pl

from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.finance.bonds import coupon_schedule

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)

#: MOEX main market -- the venue these issues pretend to trade on.
DEMO_MIC = "MISX"
DEMO_CALENDAR = "XMOS"
DEMO_CURRENCY = "RUB"

#: Coupons are paid to whoever holds the bond at the close of this many days before payment.
RECORD_DATE_LAG = datetime.timedelta(days=1)


@dataclass(frozen=True)
class DemoBondSpec:
    """The terms of one demo issue, and what it is there to demonstrate."""

    ticker: str
    name: str
    description: str
    issue_date: datetime.date
    maturity_date: datetime.date
    face_value: float = 1000.0
    coupon_rate: float = 0.0
    coupon_frequency: int = 0
    #: ``[(date, principal repaid per bond), ...]``, oldest first.
    amortizations: tuple[tuple[datetime.date, float], ...] = ()
    #: Date of a put window, if the issue has one.
    offer_date: datetime.date | None = None
    #: Clean price the generated series starts at, as a percentage of face value.
    initial_price: float = 100.0


#: The universe. Dates are chosen so that every issue is alive across 2023-2026 and at least one
#: of each kind of event falls inside a two-year backtest window.
DEMO_BONDS: tuple[DemoBondSpec, ...] = (
    DemoBondSpec(
        ticker="ZLB26",
        name="ZL Demo Sovereign 8.5% 2026",
        description="Plain fixed-coupon issue, semi-annual coupons, repaid in full at maturity",
        issue_date=datetime.date(2022, 6, 15),
        maturity_date=datetime.date(2026, 6, 15),
        coupon_rate=0.085,
        coupon_frequency=2,
        initial_price=97.5,
    ),
    DemoBondSpec(
        ticker="ZLZ26",
        name="ZL Demo Discount 2026",
        description="Zero-coupon issue: no payouts at all, the return is the pull to par",
        issue_date=datetime.date(2022, 6, 15),
        maturity_date=datetime.date(2026, 6, 15),
        initial_price=76.0,
    ),
    DemoBondSpec(
        ticker="ZLA27",
        name="ZL Demo Corporate Amortizing 11% 2027",
        description="Quarterly coupons and principal repaid in four annual instalments",
        issue_date=datetime.date(2023, 3, 20),
        maturity_date=datetime.date(2027, 3, 20),
        coupon_rate=0.11,
        coupon_frequency=4,
        amortizations=(
            (datetime.date(2024, 3, 20), 250.0),
            (datetime.date(2025, 3, 20), 250.0),
            (datetime.date(2026, 3, 20), 250.0),
        ),
        initial_price=99.0,
    ),
    DemoBondSpec(
        ticker="ZLO27",
        name="ZL Demo Corporate 12% 2027 with put",
        description="Fixed-coupon issue with a put window partway through its life",
        issue_date=datetime.date(2023, 2, 10),
        maturity_date=datetime.date(2027, 2, 10),
        coupon_rate=0.12,
        coupon_frequency=2,
        offer_date=datetime.date(2025, 2, 10),
        initial_price=101.5,
    ),
    DemoBondSpec(
        ticker="ZLS25",
        name="ZL Demo Short 9% 2025",
        description="Short-dated issue that matures inside the example window",
        issue_date=datetime.date(2023, 4, 5),
        maturity_date=datetime.date(2025, 4, 5),
        coupon_rate=0.09,
        coupon_frequency=2,
        initial_price=98.0,
    ),
)

DEMO_BONDS_BY_TICKER = {spec.ticker: spec for spec in DEMO_BONDS}


def demo_exchange() -> ExchangeInfo:
    return ExchangeInfo(mic=DEMO_MIC, name="MOEX", canonical_name="MOSCOW EXCHANGE",
                        country_code="RU")


def demo_currency() -> Currency:
    return Currency(id=None, isin=None, asset_name=DEMO_CURRENCY, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)


def demo_identifier(ticker: str) -> str:
    """A stable stand-in for an ISIN.

    Deliberately not a valid ISIN, and deliberately not ``hash()``: Python randomises string
    hashing per process, so a hash-derived identifier would differ between runs and re-seeding
    would insert a second copy of every bond instead of recognising the ones already stored.
    """
    return f"DEMOBOND{zlib.crc32(ticker.encode()) % 10_000:04d}"


def build_bond(spec: DemoBondSpec) -> Bond:
    """The :class:`Bond` entity for one spec."""
    return Bond(
        id=None,
        isin=demo_identifier(spec.ticker),
        asset_name=spec.ticker,
        start_date=spec.issue_date,
        # A bond stops trading before the issuer repays it; redemption books on the maturity date.
        end_date=spec.maturity_date - datetime.timedelta(days=1),
        first_traded=spec.issue_date,
        auto_close_date=spec.maturity_date,
        face_value=spec.face_value,
        maturity_date=spec.maturity_date,
        coupon_rate=spec.coupon_rate,
        coupon_frequency=spec.coupon_frequency,
        quote_currency=DEMO_CURRENCY,
        day_count=DayCount.ACT_365,
        price_quotation=PriceQuotation.PERCENT_OF_FACE,
        is_amortized=bool(spec.amortizations),
    )


def build_bond_events(spec: DemoBondSpec, bond: Bond) -> list[BondEvent]:
    """The full schedule of one issue: coupons, amortization instalments and any offer window.

    Coupons of an amortizing issue are computed on the principal still outstanding, which is what
    makes the payments shrink over its life -- the detail that separates an amortizing bond from a
    bullet with an early partial repayment.
    """
    events: list[BondEvent] = []
    for coupon in coupon_schedule(bond):
        if coupon.date < spec.issue_date:
            continue
        outstanding = _outstanding_face(spec, coupon.period_start_date)
        value = outstanding * spec.coupon_rate / spec.coupon_frequency
        events.append(BondEvent(
            asset=bond,
            event_type=BondEventType.COUPON,
            date=coupon.date,
            value=round(value, 2),
            currency=DEMO_CURRENCY,
            record_date=coupon.date - RECORD_DATE_LAG,
            period_start_date=coupon.period_start_date,
            face_value=outstanding,
            value_percent=spec.coupon_rate * 100.0,
        ))

    outstanding = spec.face_value
    for date, amount in spec.amortizations:
        events.append(BondEvent(
            asset=bond,
            event_type=BondEventType.AMORTIZATION,
            date=date,
            value=amount,
            currency=DEMO_CURRENCY,
            record_date=date - RECORD_DATE_LAG,
            new_face_value=outstanding - amount,
            initial_face_value=spec.face_value,
            amortization_percent=amount / spec.face_value * 100.0,
        ))
        outstanding -= amount

    if spec.offer_date is not None:
        events.append(BondEvent(
            asset=bond,
            event_type=BondEventType.OFFER,
            date=spec.offer_date,
            value=0.0,
            currency=DEMO_CURRENCY,
            offer_type="put",
            offer_price=100.0,
            offer_start_date=spec.offer_date - datetime.timedelta(days=7),
            offer_end_date=spec.offer_date,
            offer_agent="ZL Demo Broker",
        ))

    return sorted(events, key=lambda event: (event.date, event.event_type.value))


def _outstanding_face(spec: DemoBondSpec, date: datetime.date | None) -> float:
    """Principal outstanding at ``date``, after every instalment repaid by then."""
    if date is None:
        return spec.face_value
    repaid = sum(amount for instalment_date, amount in spec.amortizations
                 if instalment_date <= date)
    return spec.face_value - repaid


def build_demo_bond_universe(tickers: list[str] | None = None) -> AssetsImport:
    """Build the whole demo universe as an :class:`AssetsImport` ready to be persisted."""
    specs = [DEMO_BONDS_BY_TICKER[t] for t in tickers] if tickers else list(DEMO_BONDS)
    exchange = demo_exchange()
    currency = demo_currency()

    bonds, events, listings = [], [], []
    for spec in specs:
        bond = build_bond(spec)
        bonds.append(bond)
        events.extend(build_bond_events(spec, bond))
        listings.append(ExchangeAsset(
            sid=None,
            symbol=spec.ticker,
            start_date=bond.start_date,
            end_date=bond.end_date,
            first_traded=bond.first_traded,
            auto_close_date=bond.auto_close_date,
            external_id=f"demo-{spec.ticker}",
            exchange=exchange,
            asset=bond,
            quote=currency,
        ))

    return AssetsImport(
        exchange_assets=listings,
        currencies=[currency],
        bonds=bonds,
        bond_events=events,
    )


def demo_clean_price(spec: DemoBondSpec, date: datetime.date) -> float:
    """Clean price of one issue on ``date``, as a percentage of the *outstanding* nominal.

    Two effects, both deliberate and both reproducible:

    * **Pull to par.** The price converges on 100 as maturity approaches, linearly in time to
      maturity. Without it a discount bond would never earn its discount and a premium bond would
      never give its premium back, which is most of what a bond does.
    * **A wobble.** A slow sine in the number of days, so a strategy has something to react to.
      Deterministic, so a test can assert an exact figure.
    """
    total_days = max(1, (spec.maturity_date - spec.issue_date).days)
    elapsed = min(max((date - spec.issue_date).days, 0), total_days)
    remaining = 1.0 - elapsed / total_days

    pulled = 100.0 + (spec.initial_price - 100.0) * remaining
    wobble = 0.9 * math.sin(elapsed / 47.0) * remaining
    return round(pulled + wobble, 4)


#: Daily volume the generated bars report. Deliberately generous: slippage models cap a fill at a
#: fraction of the bar's volume, and an example whose single buy order dribbles out over a week
#: teaches nothing about bonds.
DEMO_DAILY_VOLUME = 200_000.0


def build_demo_bond_bars(listings: list[ExchangeAsset], sessions: list[datetime.date],
                         volume: float = DEMO_DAILY_VOLUME) -> pl.DataFrame:
    """Daily bars for the demo issues, in the shape a :class:`DataBundle` expects.

    Sessions outside an issue's tradeable life are skipped, so a bond that has matured stops
    printing rather than flatlining at par -- which is what a real tape does, and what makes the
    redemption path worth testing.
    """
    rows = []
    for listing in listings:
        spec = DEMO_BONDS_BY_TICKER.get(listing.symbol)
        if spec is None:
            continue
        for session in sessions:
            if session < listing.start_date or session > listing.end_date:
                continue
            price = demo_clean_price(spec, session)
            rows.append({
                "date": session,
                "sid": listing.sid,
                "symbol": listing.symbol,
                "mic": listing.mic,
                "open": price, "high": price, "low": price, "close": price, "price": price,
                "volume": volume,
            })
    return pl.DataFrame(rows)
