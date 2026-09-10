import datetime
import polars as pl
from abc import abstractmethod, ABC

from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_source import DataSource

from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.domain.order import Order
# from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.constants.period import Period


class Exchange(DataSource, ABC):

    def __init__(self, name: str, canonical_name: str, country_code: str,
                 clock: TradingClock,
                 trading_calendar: ExchangeCalendar,
                 account_id: str,
                 is_default: bool,
                 data_source: DataBundle | None = None,
                 ):
        self.name = name
        self.canonical_name = canonical_name
        self.country_code = country_code
        self.clock = clock
        self.trading_calendar = trading_calendar
        self.account_id = account_id
        self.is_default = is_default
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
    async def get_positions(self) -> dict[ExchangeAsset, Position]:
        ...

    @abstractmethod
    async def get_portfolio(self) -> Portfolio:
        ...

    @abstractmethod
    async def get_account(self) -> Account:
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
    async def get_orders(self) -> dict[str, Order]:
        ...

    @abstractmethod
    async def get_transactions(self, orders: dict[ExchangeAsset, dict[str, Order]], current_dt: datetime.datetime,
                               same_bar_execution: bool):
        ...

    @abstractmethod
    async def get_orders_by_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    async def get_transactions_by_order_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        ...

    @abstractmethod
    def get_last_traded_dt(self, asset: ExchangeAsset):
        ...

    @abstractmethod
    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime,
                             data_frequency: datetime.timedelta | Period = None):
        ...

    @abstractmethod
    def get_slippage_model(self, asset: ExchangeAsset):
        ...

    @abstractmethod
    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        ...

    # @abstractmethod
    # async def get_scalar_asset_spot_value(self, asset: ExchangeAsset, field: str, dt: datetime.datetime,
    #                                       frequency: datetime.timedelta): ...

    # @abstractmethod
    # def get_scalar_asset_spot_value_sync(self, asset: ExchangeAsset, field: str, dt: datetime.datetime,
    #                                      frequency: datetime.timedelta): ...

    # @abstractmethod
    # async def get_spot_values(self, assets: frozenset[ExchangeAsset], fields: frozenset[str]): ...

    # @abstractmethod
    # async def current(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime = None): ...

    async def current_contract(self, continuous_future, dt: datetime.datetime):
        """Return the contract a continuous future holds at ``dt``.

        Exchanges sit in front of the data bundle in ``BarData.data_sources``, so continuous-future
        lookups reach them first and are forwarded to the bundle that can answer them.
        """
        return await self.data_source.current_contract(continuous_future=continuous_future, dt=dt)

    async def get_current_future_chain(self, continuous_future, dt: datetime.datetime):
        """Return the active contracts of a chain at ``dt``, front contract first."""
        return await self.data_source.get_current_future_chain(
            continuous_future=continuous_future, dt=dt)

    async def get_data_by_limit(self, fields: frozenset[str],
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta,
                                assets: frozenset[ExchangeAsset],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        ...

    def __hash__(self):
        return hash(self.name)
