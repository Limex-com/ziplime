"""Synthetic bond fixtures for the acceptance suite.

Vendor-independent, like the futures fixtures: the invariants under test belong to the
backtester, not to any data connector.
"""
import datetime

import pandas as pd

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.finance.bonds import BondBook, coupon_schedule
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.transaction import Transaction

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)

EXCHANGE = ExchangeInfo(mic="XNYS", name="NYSE", canonical_name="NYSE",
                        country_code="US")
USD = Currency(id=1, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
               first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)

ISSUE_DATE = datetime.date(2023, 1, 10)
MATURITY_DATE = datetime.date(2026, 1, 10)


def make_bond(sid: int = 501, symbol: str = "TESTBOND", face_value: float = 1000.0,
              coupon_rate: float = 0.08, coupon_frequency: int = 2,
              issue_date: datetime.date = ISSUE_DATE,
              maturity_date: datetime.date = MATURITY_DATE,
              price_quotation: PriceQuotation = PriceQuotation.PERCENT_OF_FACE,
              day_count: DayCount = DayCount.ACT_365,
              is_amortized: bool = False) -> ExchangeAsset:
    """Build a tradeable bond listing.

    ``end_date`` is the day before maturity -- a bond stops trading before the issuer repays it --
    and ``auto_close_date`` is the maturity date itself, which is when redemption books.
    """
    bond = Bond(
        id=sid + 10_000, isin=f"RU000{sid:09d}", asset_name=symbol,
        start_date=issue_date, end_date=maturity_date - datetime.timedelta(days=1),
        first_traded=issue_date, auto_close_date=maturity_date,
        face_value=face_value, maturity_date=maturity_date, coupon_rate=coupon_rate,
        coupon_frequency=coupon_frequency, quote_currency="USD", day_count=day_count,
        price_quotation=price_quotation, is_amortized=is_amortized,
    )
    return ExchangeAsset(
        sid=sid, symbol=symbol, start_date=issue_date,
        end_date=maturity_date - datetime.timedelta(days=1), first_traded=issue_date,
        auto_close_date=maturity_date, external_id="", exchange=EXCHANGE, asset=bond, quote=USD)


def make_zero_coupon_bond(sid: int = 502, symbol: str = "ZEROBOND", **kwargs) -> ExchangeAsset:
    """A discount bond: no coupons, the whole return is the price pulling to par."""
    return make_bond(sid=sid, symbol=symbol, coupon_rate=0.0, coupon_frequency=0, **kwargs)


def make_coupon_events(listing: ExchangeAsset, record_lag_days: int = 1) -> list[BondEvent]:
    """The bond's own coupon schedule, with a record date ``record_lag_days`` before each payment."""
    bond = listing.asset
    return [
        BondEvent(
            asset=bond, event_type=BondEventType.COUPON, date=event.date, value=event.value,
            currency=bond.quote_currency,
            record_date=event.date - datetime.timedelta(days=record_lag_days),
            period_start_date=event.period_start_date, face_value=bond.face_value,
            value_percent=bond.coupon_rate * 100.0,
        )
        for event in coupon_schedule(bond)
    ]


def make_amortization_events(listing: ExchangeAsset,
                             instalments: list[tuple[datetime.date, float]]) -> list[BondEvent]:
    """Amortization instalments as ``[(date, amount repaid per bond), ...]``, oldest first."""
    bond = listing.asset
    events = []
    face = bond.face_value
    for date, amount in instalments:
        new_face = face - amount
        events.append(BondEvent(
            asset=bond, event_type=BondEventType.AMORTIZATION, date=date, value=amount,
            currency=bond.quote_currency, record_date=date - datetime.timedelta(days=1),
            new_face_value=new_face, initial_face_value=bond.face_value,
            amortization_percent=amount / bond.face_value * 100.0,
        ))
        face = new_face
    return events


def make_ledger(cash: float = 1_000_000.0, sessions: list[datetime.date] | None = None) -> Ledger:
    """A ledger with a fixed starting balance and a calendar covering the fixture bonds."""
    sessions = sessions or [ISSUE_DATE, MATURITY_DATE]
    ledger = Ledger(
        trading_sessions=pd.DatetimeIndex(sessions),
        data_frequency=datetime.timedelta(days=1),
    )
    ledger._portfolio.cash = cash
    ledger._portfolio.starting_cash = cash
    ledger._portfolio.portfolio_value = cash
    return ledger


def load_schedule(ledger: Ledger, listing: ExchangeAsset,
                  events: list[BondEvent] | None = None) -> None:
    """Put a bond's schedule into the ledger's book, as an ingest would have."""
    bond = listing.asset
    ledger.bond_book.add(bond.id, events if events is not None else make_coupon_events(listing))


def trade(ledger: Ledger, asset: ExchangeAsset, amount: int, price: float,
          dt: datetime.date | datetime.datetime, commission: float = 0.0) -> None:
    """Execute a transaction at a quoted price and mark the resulting position at it."""
    when = dt if isinstance(dt, datetime.datetime) else datetime.datetime.combine(
        dt, datetime.time.min, tzinfo=datetime.timezone.utc)
    ledger.process_transaction(Transaction(
        id=f"{asset.symbol}-{amount}-{price}-{when.date()}",
        amount=amount, dt=when, price=price, exchange_name=EXCHANGE.mic,
        trading_account_id="account-1", asset=asset))
    if commission:
        ledger.process_commission(Commission(asset=asset, order=None, amount=commission), tr=None)
    position = ledger.position_tracker.get_position(asset)
    if position is not None:
        ledger.position_tracker.update_position(
            asset=asset, exchange_name=EXCHANGE.mic, trading_account_id="account-1",
            last_sale_price=price, last_sale_date=when)


def mark(ledger: Ledger, asset: ExchangeAsset, price: float,
         dt: datetime.date | datetime.datetime) -> None:
    """Move a position's mark to ``price`` as of ``dt`` (what a new bar does)."""
    when = dt if isinstance(dt, datetime.datetime) else datetime.datetime.combine(
        dt, datetime.time.min, tzinfo=datetime.timezone.utc)
    ledger.position_tracker.update_position(
        asset=asset, exchange_name=EXCHANGE.mic, trading_account_id="account-1",
        last_sale_price=price, last_sale_date=when)
    ledger._dirty_portfolio = True


async def settle(ledger: Ledger) -> None:
    """Run the daily portfolio update."""
    ledger._dirty_portfolio = True
    await ledger.update_portfolio()


class StubBondService:
    """Enough of AssetService for the bond schedule machinery."""

    def __init__(self, events_by_bond_id: dict[int, list[BondEvent]] | None = None):
        self._events = events_by_bond_id or {}

    async def get_bond_events(self, bonds) -> dict[int, list[BondEvent]]:
        return {bond.id: self._events.get(bond.id, []) for bond in bonds}

    async def get_cash_dividends_with_ex_date(self, assets, date):
        return []


def book_with(listing: ExchangeAsset, events: list[BondEvent] | None = None) -> BondBook:
    """A stand-alone book holding one bond's schedule, for pricing tests."""
    book = BondBook()
    book.add(listing.asset.id, events if events is not None else make_coupon_events(listing))
    return book
