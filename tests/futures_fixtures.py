"""Synthetic futures fixtures for the acceptance suite.

Everything here is vendor-independent: the invariants under test belong to the backtester, not to
any data connector.
"""
import datetime

import pandas as pd

from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.equity import Equity
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.transaction import Transaction

FAR_PAST = datetime.date(1900, 1, 1)
FAR_FUTURE = datetime.date(2099, 1, 1)

EXCHANGE = ExchangeInfo(mic="XCME", name="CME", canonical_name="CME", country_code="US")
USD = Currency(id=1, isin=None, asset_name="USD", start_date=FAR_PAST, end_date=FAR_FUTURE,
               first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
UNDERLYING = Commodity(id=2, isin=None, asset_name="CL", start_date=FAR_PAST, end_date=FAR_FUTURE,
                       first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)


def make_future(sid: int, symbol: str = "CLF24", multiplier: float = 1000.0,
                tick_size: float = 0.01, root_symbol: str = "CL",
                start: datetime.date = datetime.date(2023, 1, 3),
                expiration: datetime.date = datetime.date(2023, 12, 20),
                notice: datetime.date | None = None,
                auto_close: datetime.date | None = None,
                settlement_type: SettlementType = SettlementType.CASH,
                margin_currency: str = "USD") -> ExchangeAsset:
    """Build a tradeable futures listing.

    ``auto_close`` defaults to the session after expiration: the contract trades through its
    expiration date and an open position is liquidated afterwards.
    """
    contract = FuturesContract(
        id=sid + 10_000, isin=None, asset_name=symbol, start_date=start, end_date=expiration,
        first_traded=start, auto_close_date=auto_close or (expiration + datetime.timedelta(days=1)),
        root_exchange_asset=None, root_asset=UNDERLYING, root_symbol=root_symbol,
        notice_date=notice or expiration, expiration_date=expiration,
        multiplier=multiplier, tick_size=tick_size,
        settlement_type=settlement_type, margin_currency=margin_currency,
    )
    return ExchangeAsset(
        sid=sid, symbol=symbol, start_date=start, end_date=expiration, first_traded=start,
        auto_close_date=auto_close or (expiration + datetime.timedelta(days=1)),
        external_id="", exchange=EXCHANGE, asset=contract, quote=USD)


def make_equity(sid: int = 900, symbol: str = "SPY") -> ExchangeAsset:
    """Build a tradeable equity listing, for contrast with futures accounting."""
    equity = Equity(id=sid + 10_000, isin=None, asset_name=symbol, start_date=FAR_PAST,
                    end_date=FAR_FUTURE, first_traded=FAR_PAST, auto_close_date=FAR_FUTURE)
    return ExchangeAsset(sid=sid, symbol=symbol, start_date=FAR_PAST, end_date=FAR_FUTURE,
                         first_traded=FAR_PAST, auto_close_date=FAR_FUTURE, external_id="",
                         exchange=EXCHANGE, asset=equity, quote=USD)


def make_ledger(cash: float = 1_000_000.0) -> Ledger:
    """A ledger with a fixed starting balance and a two-session calendar."""
    ledger = Ledger(
        trading_sessions=pd.DatetimeIndex([datetime.date(2023, 6, 1), datetime.date(2023, 6, 2)]),
        data_frequency=datetime.timedelta(days=1),
    )
    ledger._portfolio.cash = cash
    ledger._portfolio.starting_cash = cash
    ledger._portfolio.portfolio_value = cash
    return ledger


def trade(ledger: Ledger, asset: ExchangeAsset, amount: int, price: float,
          dt: datetime.datetime | None = None, commission: float = 0.0) -> None:
    """Execute a transaction and mark the resulting position at the traded price."""
    ledger.process_transaction(Transaction(
        id=f"{asset.symbol}-{amount}-{price}",
        amount=amount,
        dt=dt or datetime.datetime(2023, 6, 1, tzinfo=datetime.timezone.utc),
        price=price, exchange_name="XCME", trading_account_id="account-1", asset=asset))
    if commission:
        ledger.process_commission(
            Commission(asset=asset, order=None, amount=commission), tr=None)
    position = ledger.position_tracker.get_position(asset)
    if position is not None:
        ledger.position_tracker.update_position(
            asset=asset, exchange_name="XCME", trading_account_id="account-1",
            last_sale_price=price)


def mark(ledger: Ledger, asset: ExchangeAsset, price: float) -> None:
    """Move a position's mark to ``price`` (what a new bar does)."""
    ledger.position_tracker.update_position(
        asset=asset, exchange_name="XCME", trading_account_id="account-1", last_sale_price=price)
    ledger._dirty_portfolio = True


async def settle(ledger: Ledger) -> None:
    """Run the daily portfolio update, which settles futures variation margin."""
    ledger._dirty_portfolio = True
    await ledger.update_portfolio()


# --------------------------------------------------------------------------------------------
# Synthetic bundles
# --------------------------------------------------------------------------------------------
import polars as pl  # noqa: E402

from ziplime.assets.domain.continuous_future import ContinuousFuture  # noqa: E402
from ziplime.assets.domain.ordered_contracts import OrderedContracts  # noqa: E402
from ziplime.assets.utils import _encode_continuous_future_sid  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402


class StubAssetService:
    """Enough of AssetService for the roll machinery."""

    def __init__(self, contracts: list[ExchangeAsset]):
        self._contracts = contracts

    async def get_ordered_contracts(self, root_symbol: str, mic: str | None = None):
        return OrderedContracts(root_symbol=root_symbol,
                                contracts=[c for c in self._contracts
                                           if c.asset.root_symbol == root_symbol])

    async def create_continuous_future(self, root_symbol: str, offset: int = 0,
                                       roll_style: str = "volume",
                                       adjustment: str | None = "mul") -> ContinuousFuture:
        ordered = await self.get_ordered_contracts(root_symbol)
        return ContinuousFuture(
            sid=_encode_continuous_future_sid(
                root_symbol=root_symbol, offset=offset, roll_style=roll_style,
                adjustment_style={"mul": "div"}.get(adjustment, adjustment)),
            root_symbol=root_symbol, offset=offset, roll_style=roll_style, adjustment=adjustment,
            start_date=ordered.start_date, end_date=ordered.end_date, exchange_info=EXCHANGE)


def make_bundle(bars: dict[ExchangeAsset, list[tuple[datetime.date, float, float]]],
                calendar_name: str = "XNYS", asset_service=None) -> DataBundle:
    """Build an in-memory bundle from ``{listing: [(session, close, volume), ...]}``."""
    calendar = get_calendar(calendar_name)
    rows = []
    for listing, series in bars.items():
        for session, close, volume in series:
            rows.append({
                "date": datetime.datetime.combine(session, datetime.time.min,
                                                  tzinfo=calendar.tz),
                "sid": listing.sid, "symbol": listing.symbol, "mic": listing.mic,
                "open": close, "high": close, "low": close, "close": close,
                "price": close, "volume": float(volume),
            })
    frame = pl.DataFrame(rows).sort(["sid", "date"])
    indexes = frame.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}

    sessions = sorted({row["date"] for row in rows})
    return DataBundle(
        name="synthetic", version="1", start_date=sessions[0], end_date=sessions[-1],
        trading_calendar=calendar, frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=sessions[-1], data=frame, sid_indexes=sid_indexes,
        asset_service=asset_service or StubAssetService(list(bars)))


def session(bundle: DataBundle, day: datetime.date) -> datetime.datetime:
    """A tz-aware timestamp for ``day`` in the bundle's calendar."""
    return datetime.datetime.combine(day, datetime.time.min, tzinfo=bundle.trading_calendar.tz)
