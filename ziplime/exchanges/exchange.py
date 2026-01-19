import datetime
import polars as pl
from abc import abstractmethod

from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.asset import Asset
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_source import DataSource

from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
# from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.gens.domain.trading_clock import TradingClock


class Exchange(DataSource):

    def __init__(self, name: str, canonical_name: str, country_code: str,
                 clock: TradingClock,
                 trading_calendar: ExchangeCalendar,
                 data_source: DataBundle | None = None):
        self.name = name
        self.canonical_name = canonical_name
        self.country_code = country_code
        self.clock = clock
        self.trading_calendar = trading_calendar
        self.data_source = data_source

    def get_start_cash_balance(self):
        pass

    def get_current_cash_balance(self):
        pass

    def subscribe_to_market_data(self, asset):
        return []

    def get_subscribed_assets(self):
        return []

    @abstractmethod
    def get_positions(self) -> dict[Asset, Position]:
        ...

    @abstractmethod
    def get_portfolio(self) -> Portfolio:
        ...

    @abstractmethod
    def get_account(self) -> Account:
        ...

    @abstractmethod
    def get_time_skew(self):
        ...

    @abstractmethod
    async def submit_order(self, order: Order):
        ...

    def is_alive(self):
        ...

    @abstractmethod
    def get_orders(self) -> dict[str, Order]:
        ...

    @abstractmethod
    async def get_transactions(self, orders: dict[Asset, dict[str, Order]], current_dt: datetime.datetime):
        ...

    @abstractmethod
    def get_orders_by_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    def get_transactions_by_order_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        ...

    @abstractmethod
    def get_last_traded_dt(self, asset):
        ...

    @abstractmethod
    def get_spot_value(self, assets, field, dt, data_frequency):
        ...

    @abstractmethod
    def get_realtime_bars(self, assets, frequency):
        ...

    @abstractmethod
    def get_slippage_model(self, asset: Asset):
        ...

    @abstractmethod
    def get_commission_model(self, asset: Asset) -> CommissionModel:
        ...

    @abstractmethod
    async def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                          frequency: datetime.timedelta): ...

    @abstractmethod
    def get_scalar_asset_spot_value_sync(self, asset: Asset, field: str, dt: datetime.datetime,
                                         frequency: datetime.timedelta): ...

    async def get_spot_values(self, assets: frozenset[Asset], fields: frozenset[str], exchange_name: str): ...

    @abstractmethod
    def current(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime): ...

    def get_data_by_limit(self, fields: frozenset[str],
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        ...
