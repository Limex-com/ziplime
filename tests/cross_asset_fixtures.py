"""Fixtures for cross-asset tests: an equity, a bond and a futures contract on one book.

Deliberately one exchange, one currency and one calendar, so that nothing in a test can be
explained away by a venue or an FX difference. What is left is the only thing under test: three
instrument types whose accounting rules disagree with each other, sharing one ledger.
"""
import datetime

import pandas as pd

from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.transaction import Transaction

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)

EXCHANGE = ExchangeInfo(mic="MISX", name="MOEX", canonical_name="MOEX", country_code="RU")
ACCOUNT = "account-1"
RUB = Currency(id=1, isin=None, asset_name="RUB", start_date=FAR_PAST, end_date=FAR_FUTURE,
               first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
UNDERLYING = Commodity(id=2, isin=None, asset_name="USD/RUB", start_date=FAR_PAST,
                       end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)

#: A session every fixture instrument is alive on.
SESSION = datetime.date(2025, 3, 3)


def _listing(sid: int, symbol: str, asset, start=FAR_PAST, end=FAR_FUTURE,
             auto_close=None) -> ExchangeAsset:
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=start, end_date=end,
                         first_traded=start, auto_close_date=auto_close or end,
                         external_id="", exchange=EXCHANGE, asset=asset, quote=RUB)


def make_equity(sid: int = 701, symbol: str = "SBER") -> ExchangeAsset:
    """A share: the quote is money per unit, and buying it pays that money."""
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return _listing(sid, symbol, equity)


def make_bond(sid: int = 702, symbol: str = "OFZ", face_value: float = 1000.0,
              coupon_rate: float = 0.08, coupon_frequency: int = 2,
              maturity: datetime.date = datetime.date(2030, 3, 3)) -> ExchangeAsset:
    """A bond: the quote is a percentage of face value, and the buyer also owes accrued interest."""
    bond = Bond(id=sid + 10_000, isin=None, asset_name=symbol,
                start_date=datetime.date(2020, 3, 3),
                end_date=maturity - datetime.timedelta(days=1),
                first_traded=datetime.date(2020, 3, 3), auto_close_date=maturity,
                face_value=face_value, maturity_date=maturity, coupon_rate=coupon_rate,
                coupon_frequency=coupon_frequency, quote_currency="RUB",
                day_count=DayCount.ACT_365, price_quotation=PriceQuotation.PERCENT_OF_FACE)
    return _listing(sid, symbol, bond, start=datetime.date(2020, 3, 3),
                    end=maturity - datetime.timedelta(days=1), auto_close=maturity)


def make_future(sid: int = 703, symbol: str = "SiM5", multiplier: float = 1.0,
                expiration: datetime.date = datetime.date(2025, 6, 19)) -> ExchangeAsset:
    """A futures contract: no cash changes hands on opening, and P&L settles daily."""
    contract = FuturesContract(
        id=sid + 10_000, isin=None, asset_name=symbol,
        start_date=datetime.date(2023, 6, 19), end_date=expiration,
        first_traded=datetime.date(2023, 6, 19),
        auto_close_date=expiration + datetime.timedelta(days=1),
        root_exchange_asset=None, root_asset=UNDERLYING, root_symbol="Si",
        notice_date=expiration, expiration_date=expiration,
        multiplier=multiplier, tick_size=1.0,
        settlement_type=SettlementType.CASH, margin_currency="RUB")
    return _listing(sid, symbol, contract, start=datetime.date(2023, 6, 19), end=expiration,
                    auto_close=expiration + datetime.timedelta(days=1))


def make_ledger(cash: float = 10_000_000.0, futures_margin_model=None) -> Ledger:
    """A ledger with a fixed starting balance, ready to hold all three classes."""
    ledger = Ledger(
        trading_sessions=pd.DatetimeIndex([SESSION, SESSION + datetime.timedelta(days=1)]),
        data_frequency=datetime.timedelta(days=1),
        futures_margin_model=futures_margin_model,
    )
    ledger._portfolio.cash = cash
    ledger._portfolio.starting_cash = cash
    ledger._portfolio.portfolio_value = cash
    return ledger


def trade(ledger: Ledger, asset: ExchangeAsset, amount: int, price: float,
          dt: datetime.date = SESSION, commission: float = 0.0) -> None:
    """Execute a transaction at a quoted price and mark the position at it."""
    when = datetime.datetime.combine(dt, datetime.time.min, tzinfo=datetime.timezone.utc)
    ledger.process_transaction(Transaction(
        id=f"{asset.symbol}-{amount}-{price}-{dt}", amount=amount, dt=when, price=price,
        exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT, asset=asset))
    if commission:
        ledger.process_commission(Commission(asset=asset, order=None, amount=commission), tr=None)
    mark(ledger, asset, price, dt)


def mark(ledger: Ledger, asset: ExchangeAsset, price: float,
         dt: datetime.date = SESSION) -> None:
    """Move a position's mark to ``price`` as of ``dt`` (what a new bar does)."""
    when = datetime.datetime.combine(dt, datetime.time.min, tzinfo=datetime.timezone.utc)
    if ledger.position_tracker.get_position(asset) is None:
        return
    ledger.position_tracker.update_position(
        asset=asset, exchange_name=EXCHANGE.mic, trading_account_id=ACCOUNT,
        last_sale_price=price, last_sale_date=when)
    ledger._dirty_portfolio = True


async def settle(ledger: Ledger) -> None:
    """Run the daily portfolio update, which settles futures variation margin."""
    ledger._dirty_portfolio = True
    await ledger.update_portfolio()


class StubService:
    """Enough of AssetService for a mixed book: bond schedules and equity dividends."""

    def __init__(self, bond_events=None, dividends=None):
        self._bond_events = bond_events or {}
        self._dividends = dividends or []

    async def get_bond_events(self, bonds):
        return {bond.id: self._bond_events.get(bond.id, []) for bond in bonds}

    async def get_cash_dividends_with_ex_date(self, assets, date):
        asset_ids = {asset.id for asset in assets}
        return [d for d in self._dividends
                if d.asset.id in asset_ids and d.ex_date == date]
