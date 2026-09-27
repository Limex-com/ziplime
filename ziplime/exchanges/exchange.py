import datetime
import polars as pl
from abc import abstractmethod, ABC

from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_source import DataSource

from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.domain.order import Order
# from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.constants.period import Period


class Exchange(DataSource, ABC):

    #: A venue that trades for real. Its market data comes from the venue itself, so any
    #: bundle behind it is a warm-up window, not a record of where instruments stop trading.
    live: bool = False

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

    def last_available_bar(self, sid: int | None = None) -> datetime.datetime | None:
        """Where the market data behind this venue stops. Delegated to the data source.

        A live venue answers ``None``, which callers read as "no opinion" -- a live run learns
        that an instrument stopped trading from the venue, not by inspecting a frame. That holds
        even with a bundle behind it: answering with the bundle's end made every instrument read
        as delisted the day after the bundle was built, closed at its last mark and never traded
        again.
        """
        if self.live:
            return None
        data_source = getattr(self, "data_source", None)
        if data_source is None:
            return None
        return data_source.last_available_bar(sid)

    @abstractmethod
    def get_start_cash_balance(self) -> float:
        """Return the cash balance at the start of the trading run."""
        raise NotImplementedError

    @abstractmethod
    def get_current_cash_balance(self) -> float:
        """Return the venue's current cash balance."""
        raise NotImplementedError

    def subscribe_to_market_data(self, asset):
        """Subscribe to market data when the venue supports subscriptions.

        The simulation has no push feed, so the deliberately safe default is an empty
        subscription result.
        """
        return []

    def get_subscribed_assets(self):
        """Return subscribed assets; venues without push feeds return an empty list."""
        return []

    @abstractmethod
    async def get_portfolio(self) -> Portfolio:
        raise NotImplementedError

    @abstractmethod
    async def get_account(self) -> Account:
        raise NotImplementedError

    @abstractmethod
    async def submit_order(self, order: Order):
        raise NotImplementedError

    @abstractmethod
    async def get_transactions(self, orders: dict[ExchangeAsset, dict[str, Order]], current_dt: datetime.datetime,
                               same_bar_execution: bool):
        raise NotImplementedError

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime,
                             data_frequency: datetime.timedelta | Period = None):
        raise NotImplementedError

    @abstractmethod
    def get_slippage_model(self, asset: ExchangeAsset):
        ...

    @abstractmethod
    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        raise NotImplementedError

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

    @abstractmethod
    async def get_data_by_limit(self, fields: frozenset[str] | None,
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta  | Period,
                                assets: frozenset[ExchangeAsset],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        raise NotImplementedError

    def __hash__(self):
        return hash(self.name)
