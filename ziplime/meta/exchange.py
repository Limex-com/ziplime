"""The venue a metastrategy trades its strategies on.

A strategy run by a metastrategy is a listing like any other as far as the metastrategy's ledger is
concerned: it is held in units, each unit is worth the strategy's NAV, and an order for it fills at
that NAV. So the ledger, the metrics, ``context.portfolio``, leverage and the performance report of
the metastrategy all work unchanged -- a position in a strategy is a position.

The NAV is ``1 + returns`` of the strategy's own portfolio: it starts at 1.0 and moves with the
strategy's time-weighted return, so a unit is one unit of currency invested at the start, and
``order(strategy, 100_000)`` on day one is 100,000 of the strategy.
"""
import datetime
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.constants.period import Period
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.gens.domain.trading_clock import TradingClock

#: The venue name strategy units trade on, and their listings' MIC.
STRATEGY_VENUE = "STRATEGIES"
STRATEGY_ACCOUNT = "strategies"
#: ISO 4217 "no currency": a unit is priced in whatever the metastrategy's book is kept in.
_NO_CURRENCY = Currency(id=None, isin=None, asset_name="XXX", start_date=None, end_date=None,
                        first_traded=None, auto_close_date=None)
_VENUE_INFO = ExchangeInfo(mic=STRATEGY_VENUE, name=STRATEGY_VENUE, canonical_name=STRATEGY_VENUE,
                           country_code="--")


@dataclass(frozen=True)
class StrategyUnit(Asset):
    """One unit of a strategy run by a metastrategy."""


def strategy_listing(sid: int, label: str) -> ExchangeAsset:
    """The listing a strategy's units trade as. ``sid`` is negative: no bundle carries it."""
    unit = StrategyUnit(id=sid, isin=None, asset_name=label, start_date=None, end_date=None,
                        first_traded=None, auto_close_date=None)
    return ExchangeAsset(sid=sid, symbol=label, start_date=None, end_date=None, first_traded=None,
                         auto_close_date=None, external_id=label, exchange=_VENUE_INFO,
                         asset=unit, quote=_NO_CURRENCY)


def is_strategy_listing(asset) -> bool:
    return isinstance(getattr(asset, "asset", None), StrategyUnit)


class StrategyExchange(Exchange):
    """Fills orders for strategy units at the strategy's NAV, in full, on the bar they execute.

    No slippage -- the strategy's own fills already paid it -- and no commission unless the
    strategy was given one (``context.strategy(..., commission=PerDollar(0.0005))``): switching
    between strategies in a backtest is free, while in a real account it means trading their
    positions, so that is how to charge for it.
    """

    def __init__(self, clock: TradingClock, trading_calendar: ExchangeCalendar,
                 nav: Callable[[int], float],
                 commission: Callable[[int], CommissionModel | None]) -> None:
        super().__init__(name=STRATEGY_VENUE, canonical_name=STRATEGY_VENUE, country_code="--",
                         clock=clock, trading_calendar=trading_calendar,
                         account_id=STRATEGY_ACCOUNT, is_default=False, data_source=None)
        self._nav = nav
        self._commission = commission

    def get_start_cash_balance(self) -> float:
        # Units are bought with the metastrategy's cash; the venue brings none of its own.
        return 0.0

    def get_current_cash_balance(self) -> float:
        return 0.0

    async def get_portfolio(self) -> Portfolio:
        return Portfolio(cash_flow=0.0, starting_cash=0.0, portfolio_value=0.0, pnl=0.0, returns=0.0,
                         cash=0.0, positions_value=0.0, positions_exposure=0.0)

    async def get_account(self) -> Account:
        raise NotImplementedError("The strategy venue keeps no account of its own.")

    async def submit_order(self, order: Order) -> Order:
        order.id = uuid.uuid4().hex
        return order

    async def get_transactions(self, orders: dict[ExchangeAsset, dict[str, Order]],
                               current_dt: datetime.datetime, same_bar_execution: bool):
        transactions, commissions, closed = [], [], []
        for asset, asset_orders in orders.items():
            nav = self._nav(asset.sid)
            for order in list(asset_orders.values()):
                amount = order.amount - order.filled
                if not amount:
                    continue
                txn = Transaction(id=uuid.uuid4().hex, asset=asset, amount=amount, dt=current_dt,
                                  price=nav, order_id=order.id, exchange_name=self.name,
                                  trading_account_id=order.trading_account_id)
                fee = self.get_commission_model(asset).calculate(order=order, transaction=txn)
                if fee > 0:
                    commissions.append(Commission(asset=asset, order=order, amount=fee,
                                                  transaction=txn, dt=current_dt))
                order.filled += amount
                order.commission += fee
                order.dt = current_dt
                transactions.append(txn)
                if not order.open:
                    closed.append(order)
        return transactions, commissions, closed

    async def cancel_order(self, order_id: str) -> None:
        return None

    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str],
                             dt: datetime.datetime,
                             data_frequency: datetime.timedelta | Period = None) -> pl.DataFrame:
        sids = [asset.sid for asset in assets]
        navs = [self._nav(sid) for sid in sids]
        return pl.DataFrame({"sid": sids, "close": navs, "price": navs})

    def get_slippage_model(self, asset: ExchangeAsset):
        return NoSlippage()

    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        model = self._commission(asset.sid)
        return model if model is not None else NoCommission()

    async def get_data_by_limit(self, fields, limit, end_date, frequency, assets,
                                include_end_date) -> pl.DataFrame:
        # A strategy's NAV history is its performance record: `handle.performance`, not bars.
        return pl.DataFrame()
