import datetime
import importlib.util
import sys
import traceback
import uuid
from collections import namedtuple, OrderedDict
from contextlib import AsyncExitStack
from copy import copy
import warnings
from typing import Callable
import pandas as pd
import structlog

import ziplime
from itertools import chain, repeat

from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.logging_event import LoggingEvent, LoggingEvent
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.data_source import DataSource
from ziplime.domain.bar_data import BarData
from ziplime.finance.blotter.blotter import Blotter
from ziplime.finance.controls.long_only import LongOnly
from ziplime.finance.controls.max_order_count import MaxOrderCount
from ziplime.finance.controls.max_order_size import MaxOrderSize
from ziplime.finance.controls.max_position_size import MaxPositionSize
from ziplime.finance.controls.min_leverage import MinLeverage
from ziplime.finance.controls.restricted_list_order import RestrictedListOrder
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.metrics import MaxLeverage
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.exchanges.exchange import Exchange
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.trading.entities.orders.limit_order_request import LimitOrderRequest
from ziplime.trading.entities.orders.market_order_request import MarketOrderRequest
from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType
from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.trading.trading_signal_executor import TradingSignalExecutor
from ziplime.utils.calendar_utils import get_calendar

from ziplime.protocol import handle_non_market_minutes
from ziplime.errors import (
    AttachPipelineAfterInitialize,
    CannotOrderDelistedAsset,
    DuplicatePipelineName,
    IncompatibleCommissionModel,
    IncompatibleSlippageModel,
    NoSuchPipeline,
    OrderDuringInitialize,
    OrderInBeforeTradingStart,
    PipelineOutputDuringInitialize,
    RegisterAccountControlPostInit,
    RegisterTradingControlPostInit,
    ScheduleFunctionInvalidCalendar,
    SetCancelPolicyPostInit,
    SetCommissionPostInit,
    SetSlippagePostInit,
    UnsupportedCancelPolicy,
    UnsupportedDatetimeFormat,
    ZeroCapitalError, SymbolNotFound, BarSimulationError,
)

from ziplime.finance.execution import ExecutionStyle
from ziplime.finance.asset_restrictions import Restrictions
from ziplime.finance.cancel_policy import CancelPolicy
from ziplime.finance.asset_restrictions import (
    NoRestrictions,
)
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.equity import Equity
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.finance.metrics_tracker import MetricsTracker
from ziplime.pipeline import Pipeline
import ziplime.pipeline.domain as domain
from ziplime.pipeline.engine import (
    ExplodingPipelineEngine,
    SimplePipelineEngine,
)
from ziplime.utils.api_support import (
    api_method,
    require_initialized,
    require_not_initialized,
    ZiplineAPI,
    disallowed_in_before_trading_start,
)
from ziplime.utils.compat import ExitStack
from ziplime.utils.date_utils import make_utc_aware
from ziplime.utils.cache import ExpiringCache

from ziplime.utils.events import (
    EventManager,
    make_eventrule,
    date_rules,
    time_rules,
    calendars,
    AfterOpen,
    BeforeClose, EventRule,
)
from ziplime.utils.math_utils import (
    tolerant_equals,
    round_if_near_integer,
)
from ziplime.sources.benchmark_source import BenchmarkSource

# For creating and storing pipeline instances
AttachedPipeline = namedtuple("AttachedPipeline", "pipe chunks eager")


class NoBenchmark(ValueError):
    def __init__(self):
        super(NoBenchmark, self).__init__(
            "Must specify either benchmark_sid or benchmark_returns.",
        )


class TradingAlgorithm(BaseTradingAlgorithm):
    """A class that represents a trading strategy and parameters to execute
    the strategy.

    Parameters
    ----------
    *args, **kwargs
        Forwarded to ``initialize`` unless listed below.
    initialize : callable[context -> None], optional
        Function that is called at the start of the simulation to
        setup the initial context.
    handle_data : callable[(context, data) -> None], optional
        Function called on every bar. This is where most logic should be
        implemented.
    before_trading_start : callable[(context, data) -> None], optional
        Function that is called before any bars have been processed each
        day.
    analyze : callable[(context, DataFrame) -> None], optional
        Function that is called at the end of the backtest. This is passed
        the context and the performance results for the backtest.
    script : str, optional
        Algoscript that contains the definitions for the four algorithm
        lifecycle functions and any supporting code.
    namespace : dict, optional
        The namespace to execute the algoscript in. By default this is an
        empty namespace that will include only python built ins.
    algo_filename : str, optional
        The filename for the algoscript. This will be used in exception
        tracebacks. default: '<string>'.
    data_frequency : {'daily', 'minute'}, optional
        The duration of the bars.
    equities_metadata : dict or DataFrame or file-like object, optional
        If dict is provided, it must have the following structure:
        * keys are the identifiers
        * values are dicts containing the metadata, with the metadata
          field name as the key
        If pandas.DataFrame is provided, it must have the
        following structure:
        * column names must be the metadata fields
        * index must be the different asset identifiers
        * array contents should be the metadata value
        If an object with a ``read`` method is provided, ``read`` must
        return rows containing at least one of 'sid' or 'symbol' along
        with the other metadata fields.
    futures_metadata : dict or DataFrame or file-like object, optional
        The same layout as ``equities_metadata`` except that it is used
        for futures information.
    identifiers : list, optional
        Any asset identifiers that are not provided in the
        equities_metadata, but will be traded by this TradingAlgorithm.
    get_pipeline_loader : callable[BoundColumn -> PipelineLoader], optional
        The function that maps pipeline columns to their loaders.
    create_event_context : callable[BarData -> context manager], optional
        A function used to create a context mananger that wraps the
        execution of all events that are scheduled for a bar.
        This function will be passed the data for the bar and should
        return the actual context manager that will be entered.
    history_container_class : type, optional
        The type of history container to use. default: HistoryContainer
    adjustment_reader : AdjustmentReader
        The interface to the adjustments.
    """

    def __init__(
            self,
            algorithm: AlgorithmFile,
            asset_service: AssetService,
            exchanges: dict[str, Exchange],
            # Algorithm API
            metrics_set,
            blotter: Blotter,
            benchmark_source: BenchmarkSource | None,
            clock: TradingClock,
            custom_data_sources: list[DataSource],
            capital_changes=None,
            get_pipeline_loader=None,
            create_event_context=None,
            stop_on_error: bool = False
    ):
        self.algorithm = algorithm
        self.exchanges = exchanges
        self.stop_on_error = stop_on_error
        self.default_exchange = list(self.exchanges.values())[0]
        self.trading_signal_executor = TradingSignalExecutor()
        # List of trading controls to be used to validate orders.
        self.trading_controls = []

        # List of account controls to be checked on each bar.
        self.account_controls = []

        self._recorded_vars = {}
        self.namespace = {}

        # XXX: This is kind of a mess.
        # We support passing a data_portal in `run`, but we need an asset
        # finder earlier than that to look up assets for things like
        # set_benchmark.
        # self.data_portal = data_portal
        # self.market_data_provider = market_data_provider
        self.benchmark_source = benchmark_source

        # XXX: This is also a mess. We should remove all of this and only allow
        #      one way to pass a calendar.
        #
        # self.sim_params = sim_params
        self.asset_service = asset_service
        self.clock = clock

        self.metrics_tracker = None
        self._last_sync_time = pd.NaT
        self._metrics_set = metrics_set

        # Initialize Pipeline API data.
        self.init_engine(get_pipeline_loader)
        self._pipelines = {}

        # Create an already-expired cache so that we compute the first time
        # data is requested.
        self._pipeline_cache = ExpiringCache()

        self.blotter = blotter
        self.new_orders = OrderedDict()
        # The symbol lookup date specifies the date to use when resolving
        # symbols to sids, and can be set using set_symbol_lookup_date()
        self._symbol_lookup_date = None

        self._initialize = None
        self._before_trading_start = None
        self._analyze = None

        self._in_before_trading_start = False

        self.event_manager = EventManager(create_event_context)

        self._handle_data = None
        self._ledger = Ledger(trading_sessions=clock.sessions, exchanges=exchanges,
                              data_frequency=clock.emission_rate)

        self._initialize = algorithm.initialize
        self._handle_data = algorithm.handle_data
        self._before_trading_start = algorithm.before_trading_start
        # Optional analyze function, gets called after run
        self._analyze = algorithm.analyze

        self.event_manager.add_event(
            ziplime.utils.events.Event(
                ziplime.utils.events.Always(),
                # We pass handle_data.__func__ to get the unbound method.
                # We will explicitly pass the algorithm to bind it again.
                self.handle_data.__func__,
            ),
            prepend=True,
        )
        data_sources = {}

        # TODO: what if we add funds?
        for exchange in exchanges.values():
            data_sources[exchange.name] = exchange
            if exchange.get_start_cash_balance() <= 0:
                raise ZeroCapitalError()

        # Prepare the algo for initialization
        self.initialized = False

        # A dictionary of capital changes, keyed by timestamp, indicating the
        # target/delta of the capital changes, along with values
        self.capital_changes = capital_changes or {}

        # A dictionary of the actual capital change deltas, keyed by timestamp
        self.capital_change_deltas = {}

        self.restrictions = NoRestrictions()
        for ds in custom_data_sources:
            data_sources[ds.name] = ds
        self.current_data = BarData(
            simulation_dt_func=self.get_datetime,
            trading_calendar=self.clock.trading_calendar,
            restrictions=self.restrictions,
            data_sources=data_sources
        )

        # We don't have a datetime for the current snapshot until we
        # receive a message.
        self.simulation_dt = None

        self.clock = clock

        self._logger = structlog.get_logger(__name__)

    def init_engine(self, get_loader):
        """Construct and store a PipelineEngine from loader.

        If get_loader is None, constructs an ExplodingPipelineEngine
        """
        if get_loader is not None:
            self.engine = SimplePipelineEngine(
                get_loader,
                self.asset_service,
                self.default_pipeline_domain(self.clock.trading_calendar),
            )
        else:
            self.engine = ExplodingPipelineEngine()

    async def initialize(self, *args, **kwargs):
        """Call self._initialize with `self` made available to Zipline API
        functions.
        """
        with ZiplineAPI(self):
            await self._initialize(self, *args, **kwargs)

    def before_trading_start(self, data):
        self.compute_eager_pipelines()

        if self._before_trading_start is None:
            return

        self._in_before_trading_start = True

        with handle_non_market_minutes(
                data
        ) if self.clock.emission_rate == datetime.timedelta(minutes=1) else ExitStack():
            self._before_trading_start(self, data)

        self._in_before_trading_start = False

    async def handle_data(self, data):
        if self._handle_data:
            await self._handle_data(self, data)

    def analyze(self, perf):
        if self._analyze is None:
            return

        with ZiplineAPI(self):
            self._analyze(self, perf)

    def compute_eager_pipelines(self):
        """Compute any pipelines attached with eager=True."""
        for name, pipe in self._pipelines.items():
            if pipe.eager:
                self.pipeline_output(name)

    async def get_generator(self):
        """Override this method to add new logic to the construction
        of the generator. Overrides can use the _create_generator
        method to get a standard construction generator.
        """
        self.metrics_tracker = MetricsTracker(
            asset_service=self.asset_service,
            exchanges=self.exchanges,
            trading_calendar=self.clock.trading_calendar,
            sessions=self.clock.sessions,
            first_session=self.clock.start_session,
            last_session=self.clock.end_session,
            emission_rate=self.clock.emission_rate,
            ledger=self._ledger,
            metrics=self._metrics_set,
            benchmark_source=self.benchmark_source
        )

        # Set the dt initially to the period start by forcing it to change.
        # self.on_dt_changed(dt=self.clock.start_session)
        # self.datetime =self.clock.start_session
        self.simulation_dt = self.clock.start_session
        if not self.initialized:
            await self.initialize()
            self.initialized = True
        self.metrics_tracker.handle_start_of_simulation()
        return self.transform()

    async def run(self):
        """Run the algorithm."""
        self._logger.info("Running algorithm")
        # HACK: I don't think we really want to support passing a data portal
        # this late in the long term, but this is needed for now for backwards
        # compat downstream.

        # Create ziplime and loop through simulated_trading.
        # Each iteration returns a perf dictionary
        try:
            perfs = []
            errors = []
            async for perf, errors in await self.get_generator():
                perfs.append(perf)

            # convert perf dict to pandas dataframe
            daily_stats = self._create_daily_stats(perfs)

            self.analyze(daily_stats)
        finally:
            self.data_portal = None
            self.metrics_tracker = None

        return daily_stats, errors

    def _create_daily_stats(self, perfs):
        # create daily and cumulative stats dataframe
        daily_perfs = []
        # TODO: the loop here could overwrite expected properties
        # of daily_perf. Could potentially raise or log a
        # warning.
        for perf in perfs:
            if "daily_perf" in perf:
                perf["daily_perf"].update(perf["daily_perf"].pop("recorded_vars"))
                perf["daily_perf"].update(perf["cumulative_risk_metrics"])
                daily_perfs.append(perf["daily_perf"])
            else:
                self.risk_report = perf

        daily_dts = pd.DatetimeIndex([p["period_close"] for p in daily_perfs])
        daily_dts = make_utc_aware(daily_dts)
        daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)
        return daily_stats

    def calculate_capital_changes(
            self, dt: datetime.datetime, emission_rate: datetime.timedelta, is_interday: bool,
            portfolio_value_adjustment: float = 0.00
    ):
        """If there is a capital change for a given dt, this means the the change
        occurs before `handle_data` on the given dt. In the case of the
        change being a target value, the change will be computed on the
        portfolio value according to prices at the given dt

        `portfolio_value_adjustment`, if specified, will be removed from the
        portfolio_value of the cumulative performance when calculating deltas
        from target capital changes.
        """

        # CHECK is try/catch faster than search?

        try:
            capital_change = self.capital_changes[dt]
        except KeyError:
            return

        self._sync_last_sale_prices()
        if capital_change["type"] == "target":
            target = capital_change["value"]
            capital_change_amount = target - (
                    self.portfolio.portfolio_value - portfolio_value_adjustment
            )

            self._logger.info(
                "Processing capital change to target %s at %s. Capital "
                "change delta is %s" % (target, dt, capital_change_amount)
            )
        elif capital_change["type"] == "delta":
            target = None
            capital_change_amount = capital_change["value"]
            self._logger.info(
                "Processing capital change of delta %s at %s"
                % (capital_change_amount, dt)
            )
        else:
            self._logger.error(
                "Capital change %s does not indicate a valid type "
                "('target' or 'delta')" % capital_change
            )
            return

        self.capital_change_deltas.update({dt: capital_change_amount})
        self._ledger.capital_change(change_amount=capital_change_amount)

        yield {
            "capital_change": {
                "date": dt,
                "type": "cash",
                "target": target,
                "delta": capital_change_amount,
            }
        }

    def add_event(self, rule: EventRule, callback: Callable):
        """Adds an event to the algorithm's EventManager.

        Parameters
        ----------
        rule : EventRule
            The rule for when the callback should be triggered.
        callback : callable[(context, data) -> None]
            The function to execute when the rule is triggered.
        """
        self.event_manager.add_event(
            event=ziplime.utils.events.Event(rule=rule, callback=callback),
        )

    @api_method
    def schedule_function(
            self,
            func: Callable,
            date_rule: EventRule = None,
            time_rule: EventRule = None,
            half_days: bool = True,
            calendar: ExchangeCalendar | None = None,
    ):
        """Schedule a function to be called repeatedly in the future.

        Parameters
        ----------
        func : callable
            The function to execute when the rule is triggered. ``func`` should
            have the same signature as ``handle_data``.
        date_rule : ziplime.utils.events.EventRule, optional
            Rule for the dates on which to execute ``func``. If not
            passed, the function will run every trading day.
        time_rule : ziplime.utils.events.EventRule, optional
            Rule for the time at which to execute ``func``. If not passed, the
            function will execute at the end of the first market minute of the
            day.
        half_days : bool, optional
            Should this rule fire on half days? Default is True.
        calendar : Sentinel, optional
            Calendar used to compute rules that depend on the trading calendar.

        See Also
        --------
        :class:`ziplime.api.date_rules`
        :class:`ziplime.api.time_rules`
        """

        # When the user calls schedule_function(func, <time_rule>), assume that
        # the user meant to specify a time rule but no date rule, instead of
        # a date rule and no time rule as the signature suggests
        if isinstance(date_rule, (AfterOpen, BeforeClose)) and not time_rule:
            warnings.warn(
                "Got a time rule for the second positional argument "
                "date_rule. You should use keyword argument "
                "time_rule= when calling schedule_function without "
                "specifying a date_rule",
                stacklevel=3,
            )

        date_rule = date_rule or date_rules.every_day()
        time_rule = (
            (time_rule or time_rules.every_minute())
            if self.clock.emission_rate == datetime.timedelta(minutes=1)
            else
            # If we are in daily mode the time_rule is ignored.
            time_rules.every_minute()
        )

        # Check the type of the algorithm's schedule before pulling calendar
        # Note that the ExchangeTradingSchedule is currently the only
        # TradingSchedule class, so this is unlikely to be hit
        if calendar is None:
            cal = self.clock.trading_calendar
        elif calendar is calendars.US_EQUITIES:
            cal = get_calendar("XNYS")
        elif calendar is calendars.US_FUTURES:
            cal = get_calendar("us_futures")
        else:
            raise ScheduleFunctionInvalidCalendar(
                given_calendar=calendar,
                allowed_calendars="[calendars.US_EQUITIES, calendars.US_FUTURES]",
            )

        self.add_event(
            rule=make_eventrule(date_rule=date_rule, time_rule=time_rule, cal=cal, half_days=half_days),
            callback=func,
        )

    @api_method
    def record(self, *args, **kwargs):
        """Track and record values each day.

        Parameters
        ----------
        **kwargs
            The names and values to record.

        Notes
        -----
        These values will appear in the performance packets and the performance
        dataframe passed to ``analyze`` and returned from
        :func:`~ziplime.run_algorithm`.
        """
        # Make 2 objects both referencing the same iterator
        args = [iter(args)] * 2

        # Zip generates list entries by calling `next` on each iterator it
        # receives.  In this case the two iterators are the same object, so the
        # call to next on args[0] will also advance args[1], resulting in zip
        # returning (a,b) (c,d) (e,f) rather than (a,a) (b,b) (c,c) etc.
        positionals = zip(*args)
        for name, value in chain(positionals, kwargs.items()):
            self._recorded_vars[name] = value

    @api_method
    def continuous_future(
            self, root_symbol_str: str, offset: int = 0, roll: str = "volume", adjustment: str = "mul"
    ):
        """Create a specifier for a continuous contract.

        Parameters
        ----------
        root_symbol_str : str
            The root symbol for the future chain.

        offset : int, optional
            The distance from the primary contract. Default is 0.

        roll_style : str, optional
            How rolls are determined. Default is 'volume'.

        adjustment : str, optional
            Method for adjusting lookback prices between rolls. Options are
            'mul', 'add', and None. Default is 'mul'.

        Returns
        -------
        continuous_future : ziplime.assets.ContinuousFuture
            The continuous future specifier.
        """
        return self.data_portal._data_bundle.asset_repository.create_continuous_future(
            root_symbol_str,
            offset,
            roll,
            adjustment,
        )

    @api_method
    async def symbol(
            self, symbol_str: str, asset_type: AssetType = AssetType.EQUITY,
            exchange_name: str = None,
            country_code: str | None = None
    ) -> Asset | None:
        """Lookup an Equity by its ticker symbol.

        Parameters
        ----------
        symbol_str : str
            The ticker symbol for the equity to lookup.
        country_code : str or None, optional
            A country to limit symbol searches to.

        Returns
        -------
        equity : ziplime.assets.Equity
            The equity that held the ticker symbol on the current
            symbol lookup date.

        Raises
        ------
        SymbolNotFound
            Raised when the symbols was not held on the current lookup date.

        See Also
        --------
        :func:`ziplime.api.set_symbol_lookup_date`
        """
        # If the user has not set the symbol lookup date,
        # use the end_session as the date for symbol->sid resolution.
        # _lookup_date = (
        #     self._symbol_lookup_date
        #     if self._symbol_lookup_date is not None
        #     else pd.Timestamp(self.sim_params.end_session).to_pydatetime().date()
        # )
        # if exchange_name is None:
        #     exchange_name = self.default_exchange.name
        if "@" in symbol_str:
            symbol, exchange_name = symbol_str.split("@")
        else:
            symbol = symbol_str

        asset = await self.asset_service.get_asset_by_symbol(symbol=symbol,
                                                             asset_type=asset_type,
                                                             exchange_name=exchange_name)
        if asset is None:
            raise SymbolNotFound(symbol=symbol_str)
        return asset

    @api_method
    def symbols(self, *args, **kwargs):
        """Lookup multuple Equities as a list.

        Parameters
        ----------
        *args : iterable[str]
            The ticker symbols to lookup.
        country_code : str or None, optional
            A country to limit symbol searches to.

        Returns
        -------
        equities : list[ziplime.assets.Equity]
            The equities that held the given ticker symbols on the current
            symbol lookup date.

        Raises
        ------
        SymbolNotFound
            Raised when one of the symbols was not held on the current
            lookup date.

        See Also
        --------
        :func:`ziplime.api.set_symbol_lookup_date`
        """
        return [self.symbol(identifier, **kwargs) for identifier in args]

    @api_method
    async def sid(self, sid: int) -> Asset | None:
        """Lookup an Asset by its unique asset identifier.

        Parameters
        ----------
        sid : int
            The unique integer that identifies an asset.

        Returns
        -------
        asset : ziplime.assets.Asset
            The asset with the given ``sid``.

        Raises
        ------
        SidsNotFound
            When a requested ``sid`` does not map to any asset.
        """
        return await self.asset_service.get_asset_by_sid(sid=sid)

    @api_method
    async def future_symbol(self, symbol: str, exchange_name: str = None) -> FuturesContract | None:
        """Lookup a futures contract with a given symbol.

        Parameters
        ----------
        symbol : str
            The symbol of the desired contract.

        Returns
        -------
        future : ziplime.assets.Future
            The future that trades with the name ``symbol``.

        Raises
        ------
        SymbolNotFound
            Raised when no contract named 'symbol' is found.
        """
        return await self.asset_service.get_futures_contract_by_symbol(
            symbol=symbol,
            exchange_name=exchange_name or self.default_exchange.name
        )

    def _calculate_order_value_amount(self, asset: Asset, value: float, exchange_name: str):
        """Calculates how many shares/contracts to order based on the type of
        asset being ordered.
        """
        # Make sure the asset exists, and that there is a last price for it.
        # FIXME: we should use BarData's can_trade logic here, but I haven't
        # yet found a good way to do that.
        normalized_date = self.clock.trading_calendar.minute_to_session(self.simulation_dt).date()

        if normalized_date < asset.start_date:
            raise CannotOrderDelistedAsset(
                msg=f"Cannot order sid={asset.sid}, as it started trading on {asset.start_date}"
            )
        elif normalized_date > asset.end_date:
            raise CannotOrderDelistedAsset(
                msg=f"Cannot order sid={asset.sid}, as it stopped trading on {asset.end_date}."
            )
        else:
            # last_price = self.current_data.current([asset], fields={"price"})["price"][0]
            last_price_data = self.exchanges[exchange_name].current(frozenset({asset}),
                                                               dt=self.simulation_dt,
                                                               fields=frozenset({"price"}))["price"]
            if len(last_price_data) == 0:
            # if last_price is None:
                raise CannotOrderDelistedAsset(
                    msg=f"Cannot order sid={asset.sid} on {self.simulation_dt} as there is no last price for the security."
                )
            last_price = last_price_data[0]
            if last_price is None:
                raise CannotOrderDelistedAsset(
                    msg=f"Cannot order sid={asset.sid} on {self.simulation_dt} as there is no last price for the security."
                )
        if tolerant_equals(last_price, 0):
            self._logger.debug(f"Price of 0 for {asset}; can't infer value")
            # Don't place any order
            return 0
        if type(asset) is FuturesContract:
            return value / (last_price * asset.multiplier)
        else:
            return value / last_price

    def _can_order_asset(self, asset: Asset):
        if asset.auto_close_date:
            day = self.clock.trading_calendar.minute_to_session(self.simulation_dt).date()

            if day > min(asset.end_date, asset.auto_close_date):
                # If we are after the asset's end date or auto close date, warn
                # the user that they can't place an order for this asset, and
                # return None.
                self._logger.warning(
                    f"Cannot place order for sid={asset.sid}, as it has de-listed. "
                    f"Any existing positions for this asset will be "
                    f"liquidated on "
                    f"{asset.auto_close_date}."
                )

                return False

        return True

    def reject_order(self, order_id: str, reason: str = ""):
        """
        Mark the given order as 'rejected', which is functionally similar to
        cancelled. The distinction is that rejections are involuntary (and
        usually include a message from a exchange indicating why the order was
        rejected) while cancels are typically user-driven.
        """
        order = self.blotter.get_order_by_id(order_id)
        if order is None:
            return
        order.reject(reason=reason)
        order.dt = self.simulation_dt

        self.blotter.order_rejected(order=order)
        # we want this order's new status to be relayed out
        # along with newly placed orders.
        self.new_orders.move_to_end(order_id)

    def hold_order(self, order_id: str, reason: str = ""):
        """
        Mark the order with order_id as 'held'. Held is functionally similar
        to 'open'. When a fill (full or partial) arrives, the status
        will automatically change back to open/filled as necessary.
        """
        order = self.blotter.get_order_by_id(order_id)
        if order is None or not order.open:
            return
        order.hold(reason=reason)
        order.dt = self.simulation_dt
        # we want this order's new status to be relayed out
        # along with newly placed orders.
        self.new_orders.move_to_end(order.id)

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order(self, asset: Asset, amount: int, style: ExecutionStyle,
                    exchange_name: str | None = None) -> Order | None:
        """Place an order for a fixed number of shares.

        Parameters
        ----------
        asset : Asset
            The asset to be ordered.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : ExecutionStyle, optional
            The execution style for the order.

        Returns
        -------
        order_id : str or None
            The unique identifier for this order, or None if no order was
            placed.

        Notes
        -----
        The ``limit_price`` and ``stop_price`` arguments provide shorthands for
        passing common execution styles. Passing ``limit_price=N`` is
        equivalent to ``style=LimitOrder(N)``. Similarly, passing
        ``stop_price=M`` is equivalent to ``style=StopOrder(M)``, and passing
        ``limit_price=N`` and ``stop_price=M`` is equivalent to
        ``style=StopLimitOrder(N, M)``. It is an error to pass both a ``style``
        and ``limit_price`` or ``stop_price``.

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order_value`
        :func:`ziplime.api.order_percent`
        """
        if not self._can_order_asset(asset=asset):
            return None
        # TODO: implement dynamic risk control

        self.validate_order_params(asset=asset, amount=amount)
        if exchange_name is None:
            exchange = self.default_exchange
        else:
            exchange = self.exchanges[exchange_name]
        order_id = None
        order_qty_rounded = int(round_if_near_integer(amount))

        order = Order(
            dt=self.simulation_dt,
            asset=asset,
            amount=order_qty_rounded,
            id=order_id,
            commission=0.00,
            filled=0,
            execution_style=style,
            status=OrderStatus.OPEN,
            exchange_name=exchange.name
        )
        if amount == 0:
            self._logger.warning("Not executing order for zero shares.")
            return None

        quote_asset = await self.asset_service.get_currency_by_symbol(symbol="USD",
                                                                      exchange_name=exchange_name)

        self._logger.info(LoggingEvent.ORDER_SUBMIT, style=str(style), quantity=order_qty_rounded,
                          symbol=asset.get_symbol_by_exchange(exchange_name),
                          simulation_dt=self.simulation_dt)

        submitted_order = await self.default_exchange.submit_order(order=order)
        # quote_asset = await self.asset_service.get_currency_by_symbol(symbol="USD",
        #                                                               exchange_name=exchange_name)

        # match style.to_order_type():
        #     case OrderType.LIMIT:
        #         order_req = LimitOrderRequest(
        #             order_id=uuid.uuid4().hex,
        #             trading_pair=TradingPair(base_asset=asset,
        #                                      quote_asset=quote_asset,
        #                                      exchange_name=exchange.name),
        #             order_side=OrderSide.BUY if amount > 0 else OrderSide.SELL,
        #             quantity=float(amount),
        #             limit_price=style.get_limit_price(is_buy=amount > 0)
        #         )
        #     case OrderType.MARKET:
        #         order_req = MarketOrderRequest(
        #             order_id=uuid.uuid4().hex,
        #             trading_pair=TradingPair(base_asset=asset,
        #                                      quote_asset=quote_asset,
        #                                      exchange_name=exchange.name),
        #             order_side=OrderSide.BUY if amount > 0 else OrderSide.SELL,
        #             quantity=float(amount),
        #             exchange_name=exchange.name,
        #             creation_date=self.simulation_dt
        #         )
        #     case _:
        #         raise ValueError(f"Unsupported order type: {style.to_order_type()}")

        # trading_signal = await self.trading_signal_executor.create_order_execute_trading_signal(
        #     algorithm=self,
        #     order=order_req,
        #     exchange=exchange
        # )

        persisted_order = self.blotter.save_order(order=submitted_order)
        self.new_orders[submitted_order.id] = submitted_order

        return submitted_order

    def new_order_submitted(self, order: Order):
        self.blotter.save_order(order=order)
        self.new_orders[order.id] = order
        return order

    def validate_order_params(self, asset: Asset, amount: int):
        """
        Helper method for validating parameters to the order API function.

        Raises an UnsupportedOrderParameters if invalid arguments are found.
        """

        if not self.initialized:
            raise OrderDuringInitialize(
                msg="order() can only be called from within handle_data()"
            )

        for control in self.trading_controls:
            control.validate(
                asset=asset,
                amount=amount,
                portfolio=self.portfolio,
                algo_datetime=self.simulation_dt,
                algo_current_data=self.current_data,
            )

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_value(self, asset: Asset, value: float, limit_price: float | None = None,
                          stop_price: float | None = None,
                          style: ExecutionStyle | None = None,
                          exchange_name: str | None = None
                          ):
        """Place an order for a fixed amount of money.

        Equivalent to ``order(asset, value / data.current(asset, 'price'))``.

        Parameters
        ----------
        asset : Asset
            The asset to be ordered.
        value : float
            Amount of value of ``asset`` to be transacted. The number of shares
            bought or sold will be equal to ``value / current_price``.
        limit_price : float, optional
            Limit price for the order.
        stop_price : float, optional
            Stop price for the order.
        style : ExecutionStyle
            The execution style for the order.

        Returns
        -------
        order_id : str
            The unique identifier for this order.

        Notes
        -----
        See :func:`ziplime.api.order` for more information about
        ``limit_price``, ``stop_price``, and ``style``

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order`
        :func:`ziplime.api.order_percent`
        """
        if not self._can_order_asset(asset):
            return None

        amount = self._calculate_order_value_amount(asset=asset, value=value,
                                                    exchange_name=exchange_name or self.default_exchange.name)
        return await self.order(
            asset,
            amount,
            limit_price=limit_price,
            stop_price=stop_price,
            style=style,
            exchange_name=exchange_name
        )

    @property
    def recorded_vars(self):
        return copy(self._recorded_vars)

    def _sync_last_sale_prices(self, dt: datetime.datetime = None):
        """Sync the last sale prices on the metrics tracker to a given
        datetime.

        Parameters
        ----------
        dt : datetime
            The time to sync the prices to.

        Notes
        -----
        This call is cached by the datetime. Repeated calls in the same bar
        are cheap.
        """
        if dt is None:
            dt = self.simulation_dt

        if dt != self._last_sync_time:
            self._ledger.sync_last_sale_prices(dt=dt, handle_non_market_minutes=False)
            self._last_sync_time = dt

    @property
    def portfolio(self):
        self._sync_last_sale_prices()
        return self._ledger.portfolio

    @property
    def account(self):
        self._sync_last_sale_prices()
        return self._ledger.account

    # def on_dt_changed(self, dt):
    #     """Callback triggered by the simulation loop whenever the current dt
    #     changes.
    #
    #     Any logic that should happen exactly once at the start of each datetime
    #     group should happen here.
    #     """
    #     self.datetime = dt

    @api_method
    def get_datetime(self):
        """Returns the current simulation datetime.

        Parameters
        ----------
        tz : tzinfo or str, optional
            The timezone to return the datetime in. This defaults to utc.

        Returns
        -------
        dt : datetime
            The current simulation datetime converted to ``tz``.
        """
        return self.simulation_dt
        # dt = self.datetime
        # return dt

    @api_method
    def set_slippage(self, us_equities=None, us_futures=None):
        """Set the slippage models for the simulation.

        Parameters
        ----------
        us_equities : EquitySlippageModel
            The slippage model to use for trading US equities.
        us_futures : FutureSlippageModel
            The slippage model to use for trading US futures.

        Notes
        -----
        This function can only be called during
        :func:`~ziplime.api.initialize`.

        See Also
        --------
        :class:`ziplime.finance.slippage.SlippageModel`
        """
        if self.initialized:
            raise SetSlippagePostInit()

        if us_equities is not None:
            if Equity not in us_equities.allowed_asset_types:
                raise IncompatibleSlippageModel(
                    asset_type="equities",
                    given_model=us_equities,
                    supported_asset_types=us_equities.allowed_asset_types,
                )
            self.blotter.slippage_models[Equity] = us_equities

        if us_futures is not None:
            if FuturesContract not in us_futures.allowed_asset_types:
                raise IncompatibleSlippageModel(
                    asset_type="futures",
                    given_model=us_futures,
                    supported_asset_types=us_futures.allowed_asset_types,
                )
            self.blotter.slippage_models[FuturesContract] = us_futures

    @api_method
    def set_commission(self, us_equities=None, us_futures=None):
        """Sets the commission models for the simulation.

        Parameters
        ----------
        us_equities : EquityCommissionModel
            The commission model to use for trading US equities.
        us_futures : FutureCommissionModel
            The commission model to use for trading US futures.

        Notes
        -----
        This function can only be called during
        :func:`~ziplime.api.initialize`.

        See Also
        --------
        :class:`ziplime.finance.commission.PerShare`
        :class:`ziplime.finance.commission.PerTrade`
        :class:`ziplime.finance.commission.PerDollar`
        """
        if self.initialized:
            raise SetCommissionPostInit()

        if us_equities is not None:
            if Equity not in us_equities.allowed_asset_types:
                raise IncompatibleCommissionModel(
                    asset_type="equities",
                    given_model=us_equities,
                    supported_asset_types=us_equities.allowed_asset_types,
                )
            self.blotter.commission_models[Equity] = us_equities

        if us_futures is not None:
            if FuturesContract not in us_futures.allowed_asset_types:
                raise IncompatibleCommissionModel(
                    asset_type="futures",
                    given_model=us_futures,
                    supported_asset_types=us_futures.allowed_asset_types,
                )
            self.blotter.commission_models[FuturesContract] = us_futures

    @api_method
    def set_cancel_policy(self, cancel_policy):
        """Sets the order cancellation policy for the simulation.

        Parameters
        ----------
        cancel_policy : CancelPolicy
            The cancellation policy to use.

        See Also
        --------
        :class:`ziplime.api.EODCancel`
        :class:`ziplime.api.NeverCancel`
        """
        if not isinstance(cancel_policy, CancelPolicy):
            raise UnsupportedCancelPolicy()

        if self.initialized:
            raise SetCancelPolicyPostInit()

        self.blotter.cancel_policy = cancel_policy

    @api_method
    def set_symbol_lookup_date(self, dt):
        """Set the date for which symbols will be resolved to their assets
        (symbols may map to different firms or underlying assets at
        different times)

        Parameters
        ----------
        dt : datetime
            The new symbol lookup date.
        """
        try:
            self._symbol_lookup_date = pd.Timestamp(dt).tz_localize("UTC")
        except TypeError:
            self._symbol_lookup_date = pd.Timestamp(dt).tz_convert("UTC")
        except ValueError as exc:
            raise UnsupportedDatetimeFormat(
                input=dt, method="set_symbol_lookup_date"
            ) from exc

    # @property
    # def data_frequency(self):
    #     return self.sim_params.data_frequency
    #
    # @data_frequency.setter
    # def data_frequency(self, value):
    #     assert value in ("daily", "minute")
    #     self.sim_params.data_frequency = value

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_percent(
            self, asset: Asset, percent: float, style: ExecutionStyle,
            exchange_name: str | None = None
    ):
        """Place an order in the specified asset corresponding to the given
        percent of the current portfolio value.

        Parameters
        ----------
        asset : Asset
            The asset that this order is for.
        percent : float
            The percentage of the portfolio value to allocate to ``asset``.
            This is specified as a decimal, for example: 0.50 means 50%.
        style : ExecutionStyle
            The execution style for the order.

        Returns
        -------
        order_id : str
            The unique identifier for this order.

        Notes
        -----
        See :func:`ziplime.api.order` for more information about
        ``limit_price``, ``stop_price``, and ``style``

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order`
        :func:`ziplime.api.order_value`
        """
        if not self._can_order_asset(asset=asset):
            return None

        amount = self._calculate_order_percent_amount(asset=asset, percent=percent,
                                                      exchange_name=exchange_name or self.default_exchange.name)
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    def _calculate_order_percent_amount(self, asset: Asset, percent: float, exchange_name: str):
        value = self.portfolio.portfolio_value * percent
        return self._calculate_order_value_amount(asset=asset, value=value, exchange_name=exchange_name)

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target(
            self, asset: Asset, target: int, style: ExecutionStyle,
            exchange_name: str | None = None
    ):
        """Place an order to adjust a position to a target number of shares. If
        the position doesn't already exist, this is equivalent to placing a new
        order. If the position does exist, this is equivalent to placing an
        order for the difference between the target number of shares and the
        current number of shares.

        Parameters
        ----------
        asset : Asset
            The asset that this order is for.
        target : int
            The desired number of shares of ``asset``.
        limit_price : float, optional
            The limit price for the order.
        stop_price : float, optional
            The stop price for the order.
        style : ExecutionStyle
            The execution style for the order.

        Returns
        -------
        order_id : str
            The unique identifier for this order.


        Notes
        -----
        ``order_target`` does not take into account any open orders. For
        example:

        .. code-block:: python

           order_target(sid(0), 10)
           order_target(sid(0), 10)

        This code will result in 20 shares of ``sid(0)`` because the first
        call to ``order_target`` will not have been filled when the second
        ``order_target`` call is made.

        See :func:`ziplime.api.order` for more information about
        ``limit_price``, ``stop_price``, and ``style``

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order`
        :func:`ziplime.api.order_target_percent`
        :func:`ziplime.api.order_target_value`
        """
        if not self._can_order_asset(asset=asset):
            return None

        amount = self._calculate_order_target_amount(asset=asset, target=target)
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    def _calculate_order_target_amount(self, asset: Asset, target: int):
        if asset in self.portfolio.positions:
            current_position = self.portfolio.positions[asset].amount
            target -= current_position

        return target

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target_value(
            self, asset: Asset, target: float, style: ExecutionStyle,
            exchange_name: str | None = None
    ):
        """Place an order to adjust a position to a target value. If
        the position doesn't already exist, this is equivalent to placing a new
        order. If the position does exist, this is equivalent to placing an
        order for the difference between the target value and the
        current value.
        If the Asset being ordered is a Future, the 'target value' calculated
        is actually the target exposure, as Futures have no 'value'.

        Parameters
        ----------
        asset : Asset
            The asset that this order is for.
        target : float
            The desired total value of ``asset``.
        style : ExecutionStyle
            The execution style for the order.

        Returns
        -------
        order_id : str
            The unique identifier for this order.

        Notes
        -----
        ``order_target_value`` does not take into account any open orders. For
        example:

        .. code-block:: python

           order_target_value(sid(0), 10)
           order_target_value(sid(0), 10)

        This code will result in 20 dollars of ``sid(0)`` because the first
        call to ``order_target_value`` will not have been filled when the
        second ``order_target_value`` call is made.

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order`
        :func:`ziplime.api.order_target`
        :func:`ziplime.api.order_target_percent`
        """
        if not self._can_order_asset(asset):
            return None

        target_amount = self._calculate_order_value_amount(asset=asset, value=target,
                                                           exchange_name=exchange_name or self.default_exchange.name)
        amount = self._calculate_order_target_amount(asset, target_amount)
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target_percent(
            self, asset: Asset, target: float,
            style: ExecutionStyle, exchange_name: str | None = None
    ):
        """Place an order to adjust a position to a target percent of the
        current portfolio value. If the position doesn't already exist, this is
        equivalent to placing a new order. If the position does exist, this is
        equivalent to placing an order for the difference between the target
        percent and the current percent.

        Parameters
        ----------
        asset : Asset
            The asset that this order is for.
        target : float
            The desired percentage of the portfolio value to allocate to
            ``asset``. This is specified as a decimal, for example:
            0.50 means 50%.
        style : ExecutionStyle
            The execution style for the order.

        Returns
        -------
        order_id : str
            The unique identifier for this order.

        Notes
        -----
        ``order_target_value`` does not take into account any open orders. For
        example:

        .. code-block:: python

           order_target_percent(sid(0), 10)
           order_target_percent(sid(0), 10)

        This code will result in 20% of the portfolio being allocated to sid(0)
        because the first call to ``order_target_percent`` will not have been
        filled when the second ``order_target_percent`` call is made.

        See :func:`ziplime.api.order` for more information about
        ``limit_price``, ``stop_price``, and ``style``

        See Also
        --------
        :class:`ziplime.finance.execution.ExecutionStyle`
        :func:`ziplime.api.order`
        :func:`ziplime.api.order_target`
        :func:`ziplime.api.order_target_value`
        """
        if not (-1 <= target <= 1):
            raise ValueError("target must be between -1 and 1")
        if not self._can_order_asset(asset):
            return None

        target_amount = self._calculate_order_percent_amount(asset=asset, percent=target,
                                                             exchange_name=exchange_name or self.default_exchange.name)
        amount = self._calculate_order_target_amount(asset=asset, target=target_amount)

        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    # def _calculate_order_target_percent_amount(self, asset, target):
    #     target_amount = self._calculate_order_percent_amount(asset, target)
    #     return self._calculate_order_target_amount(asset, target_amount)

    # @api_method
    # def batch_market_order(self, share_counts):
    #     """Place a batch market order for multiple assets.
    #
    #     Parameters
    #     ----------
    #     share_counts : pd.Series[Asset -> int]
    #         Map from asset to number of shares to order for that asset.
    #
    #     Returns
    #     -------
    #     order_ids : pd.Index[str]
    #         Index of ids for newly-created orders.
    #     """
    #     style = MarketOrder()
    #     order_args = [
    #         (asset, amount, style) for (asset, amount) in share_counts.items() if amount
    #     ]
    #     return self.blotter.batch_order(order_args)

    @api_method
    def get_open_orders(self, asset=None):
        """Retrieve all of the current open orders.

        Parameters
        ----------
        asset : Asset
            If passed and not None, return only the open orders for the given
            asset instead of all open orders.

        Returns
        -------
        open_orders : dict[list[Order]] or list[Order]
            If no asset is passed this will return a dict mapping Assets
            to a list containing all the open orders for the asset.
            If an asset is passed then this will return a list of the open
            orders for this asset.
        """
        if asset is None:
            return {
                key: list(orders.values())  # [order.to_api_obj() for order in orders]
                for key, orders in self.blotter.open_orders.items()
                if orders
            }
        if asset in self.blotter.open_orders:
            orders = self.blotter.open_orders[asset]
            return list(orders.values())
            # return [order.to_api_obj() for order in orders]
        return []

    @api_method
    def get_order(self, order_id) -> Order | None:
        """Lookup an order based on the order id returned from one of the
        order functions.

        Parameters
        ----------
        order_id : str
            The unique identifier for the order.

        Returns
        -------
        order : Order
            The order object.
        """
        self.exchange.get_orders_by_ids([order_id])
        return self.blotter.get_order_by_id(order_id=order_id)

    @api_method
    def cancel_order(self, order_id: str, relay_status: bool = True) -> None:
        """Cancel an open order.

        Parameters
        ----------
        order_param : str or Order
            The order_id or order object to cancel.
        """
        order = self.blotter.get_order_by_id(order_id=order_id)
        if order is None or not order.open:
            return
        order.cancel()
        order.dt = self.simulation_dt
        # we want this order's new status to be relayed out
        # along with newly placed orders.

        self.blotter.order_cancelled(order=order)
        self.exchange.cancel_order(order_id=order_id)
        if relay_status:
            self.new_orders[order.id] = order
        else:
            self.new_orders.pop(order.id, None)

    def cancel_all_orders_for_asset(self, asset: Asset, warn: bool = False, relay_status: bool = True):
        """
        Cancel all open orders for a given asset.
        """
        # (sadly) open_orders is a defaultdict, so this will always succeed.
        orders = self.blotter.get_open_orders_by_asset(asset=asset)
        if not orders:
            return
        # We're making a copy here because `cancel` mutates the list of open
        # orders in place.  The right thing to do here would be to make
        # self.open_orders no longer a defaultdict.  If we do that, then we
        # should just remove the orders once here and be done with the matter.
        for order_id, order in orders.items():
            self.cancel_order(order_id=order.id, relay_status=relay_status)
            if warn:
                # Message appropriately depending on whether there's
                # been a partial fill or not.
                if order.filled > 0:
                    self._logger.warning(
                        f"Your order for {order.amount} shares of "
                        f"{order.asset.sid} has been partially filled. "
                        f"{order.filled} shares were successfully "
                        f"purchased. {order.amount - order.filled} shares were not "
                        f"filled by the end of day and "
                        f"were canceled."
                    )
                elif order.filled < 0:
                    self._logger.warning(
                        f"Your order for {order.amount} shares of "
                        f"{asset.sid} has been partially filled. "
                        f"{-1 * order.filled} shares were successfully "
                        f"sold. {-1 * (order.amount - order.filled)} shares were not "
                        f"filled by the end of day and "
                        f"were canceled."
                    )
                else:
                    self._logger.warning(
                        f"Your order for {order.amount} shares of "
                        f"{order.asset.sid} failed to fill by the end of day "
                        f"and was canceled."
                    )
        self.blotter.cancel_all_orders_for_asset(asset=asset, relay_status=relay_status)

    ####################
    # Account Controls #
    ####################

    def register_account_control(self, control):
        """
        Register a new AccountControl to be checked on each bar.
        """
        if self.initialized:
            raise RegisterAccountControlPostInit()
        self.account_controls.append(control)

    def validate_account_controls(self):
        for control in self.account_controls:
            control.validate(
                self.portfolio,
                self.account,
                self.simulation_dt,
                self.current_data,
            )

    @api_method
    def set_max_leverage(self, max_leverage):
        """Set a limit on the maximum leverage of the algorithm.

        Parameters
        ----------
        max_leverage : float
            The maximum leverage for the algorithm. If not provided there will
            be no maximum.
        """
        control = MaxLeverage(max_leverage)
        self.register_account_control(control)

    @api_method
    def set_min_leverage(self, min_leverage, grace_period):
        """Set a limit on the minimum leverage of the algorithm.

        Parameters
        ----------
        min_leverage : float
            The minimum leverage for the algorithm.
        grace_period : pd.Timedelta
            The offset from the start date used to enforce a minimum leverage.
        """
        deadline = self.sim_params.start_session + grace_period
        control = MinLeverage(min_leverage, deadline)
        self.register_account_control(control)

    ####################
    # Trading Controls #
    ####################

    def register_trading_control(self, control):
        """
        Register a new TradingControl to be checked prior to order calls.
        """
        if self.initialized:
            raise RegisterTradingControlPostInit()
        self.trading_controls.append(control)

    @api_method
    def set_max_position_size(
            self, asset=None, max_shares=None, max_notional=None, on_error="fail"
    ):
        """Set a limit on the number of shares and/or dollar value held for the
        given sid. Limits are treated as absolute values and are enforced at
        the time that the algo attempts to place an order for sid. This means
        that it's possible to end up with more than the max number of shares
        due to splits/dividends, and more than the max notional due to price
        improvement.

        If an algorithm attempts to place an order that would result in
        increasing the absolute value of shares/dollar value exceeding one of
        these limits, raise a TradingControlException.

        Parameters
        ----------
        asset : Asset, optional
            If provided, this sets the guard only on positions in the given
            asset.
        max_shares : int, optional
            The maximum number of shares to hold for an asset.
        max_notional : float, optional
            The maximum value to hold for an asset.
        """
        control = MaxPositionSize(
            asset=asset,
            max_shares=max_shares,
            max_notional=max_notional,
            on_error=on_error,
        )
        self.register_trading_control(control)

    @api_method
    def set_max_order_size(
            self, asset=None, max_shares=None, max_notional=None, on_error="fail"
    ):
        """Set a limit on the number of shares and/or dollar value of any single
        order placed for sid.  Limits are treated as absolute values and are
        enforced at the time that the algo attempts to place an order for sid.

        If an algorithm attempts to place an order that would result in
        exceeding one of these limits, raise a TradingControlException.

        Parameters
        ----------
        asset : Asset, optional
            If provided, this sets the guard only on positions in the given
            asset.
        max_shares : int, optional
            The maximum number of shares that can be ordered at one time.
        max_notional : float, optional
            The maximum value that can be ordered at one time.
        """
        control = MaxOrderSize(
            asset=asset,
            max_shares=max_shares,
            max_notional=max_notional,
            on_error=on_error,
        )
        self.register_trading_control(control)

    @api_method
    def set_max_order_count(self, max_count, on_error="fail"):
        """Set a limit on the number of orders that can be placed in a single
        day.

        Parameters
        ----------
        max_count : int
            The maximum number of orders that can be placed on any single day.
        """
        control = MaxOrderCount(on_error, max_count)
        self.register_trading_control(control)

    @api_method
    def set_asset_restrictions(self, restrictions: Restrictions, on_error: str = "fail"):
        """Set a restriction on which assets can be ordered.

        Parameters
        ----------
        restricted_list : Restrictions
            An object providing information about restricted assets.

        See Also
        --------
        ziplime.finance.asset_restrictions.Restrictions
        """
        control = RestrictedListOrder(on_error, restrictions)
        self.register_trading_control(control)
        self.restrictions |= restrictions

    @api_method
    def set_long_only(self, on_error="fail"):
        """Set a rule specifying that this algorithm cannot take short
        positions.
        """
        self.register_trading_control(LongOnly(on_error))

    ##############
    # Pipeline API
    ##############
    @api_method
    @require_not_initialized(AttachPipelineAfterInitialize())
    def attach_pipeline(self, pipeline, name, chunks=None, eager=True):
        """Register a pipeline to be computed at the start of each day.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to have computed.
        name : str
            The name of the pipeline.
        chunks : int or iterator, optional
            The number of days to compute pipeline results for. Increasing
            this number will make it longer to get the first results but
            may improve the total runtime of the simulation. If an iterator
            is passed, we will run in chunks based on values of the iterator.
            Default is True.
        eager : bool, optional
            Whether or not to compute this pipeline prior to
            before_trading_start.

        Returns
        -------
        pipeline : Pipeline
            Returns the pipeline that was attached unchanged.

        See Also
        --------
        :func:`ziplime.api.pipeline_output`
        """
        if chunks is None:
            # Make the first chunk smaller to get more immediate results:
            # (one week, then every half year)
            chunks = chain([5], repeat(126))
        elif isinstance(chunks, int):
            chunks = repeat(chunks)

        if name in self._pipelines:
            raise DuplicatePipelineName(name=name)

        self._pipelines[name] = AttachedPipeline(pipeline, iter(chunks), eager)

        # Return the pipeline to allow expressions like
        # p = attach_pipeline(Pipeline(), 'name')
        return pipeline

    @api_method
    @require_initialized(PipelineOutputDuringInitialize())
    def pipeline_output(self, name):
        """Get results of the pipeline attached by with name ``name``.

        Parameters
        ----------
        name : str
            Name of the pipeline from which to fetch results.

        Returns
        -------
        results : pd.DataFrame
            DataFrame containing the results of the requested pipeline for
            the current simulation date.

        Raises
        ------
        NoSuchPipeline
            Raised when no pipeline with the name `name` has been registered.

        See Also
        --------
        :func:`ziplime.api.attach_pipeline`
        :meth:`ziplime.pipeline.engine.PipelineEngine.run_pipeline`
        """
        try:
            pipe, chunks, _ = self._pipelines[name]
        except KeyError as exc:
            raise NoSuchPipeline(
                name=name,
                valid=list(self._pipelines.keys()),
            ) from exc
        return self._pipeline_output(pipe, chunks, name)

    def _pipeline_output(self, pipeline, chunks, name):
        """Internal implementation of `pipeline_output`."""
        # TODO FIXME TZ MESS
        today = self.simulation_dt
        try:
            data = self._pipeline_cache.get(key=name, dt=today)
        except KeyError:
            # Calculate the next block.
            data, valid_until = self.run_pipeline(
                pipeline=pipeline,
                start_session=today,
                chunksize=next(chunks),
            )
            self._pipeline_cache.set(key=name, value=data, expiration_dt=valid_until)

        # Now that we have a cached result, try to return the data for today.
        try:
            return data.loc[today]
        except KeyError:
            # This happens if no assets passed the pipeline screen on a given
            # day.
            return pd.DataFrame(index=[], columns=data.columns)

    def run_pipeline(self, pipeline, start_session, chunksize):
        """Compute `pipeline`, providing values for at least `start_date`.

        Produces a DataFrame containing data for days between `start_date` and
        `end_date`, where `end_date` is defined by:

            `end_date = min(start_date + chunksize trading days,
                            simulation_end)`

        Returns
        -------
        (data, valid_until) : tuple (pd.DataFrame, datetime.datetime)

        See Also
        --------
        PipelineEngine.run_pipeline
        """
        sessions = self.clock.trading_calendar.sessions

        # Load data starting from the previous trading day...
        start_date_loc = sessions.get_loc(start_session)

        # ...continuing until either the day before the simulation end, or
        # until chunksize days of data have been loaded.
        sim_end_session = self.sim_params.end_session

        end_loc = min(start_date_loc + chunksize, sessions.get_loc(sim_end_session))

        end_session = sessions[end_loc]

        return (
            self.engine.run_pipeline(pipeline, start_session, end_session),
            end_session,
        )

    @staticmethod
    def default_pipeline_domain(calendar):
        """Get a default pipeline domain for algorithms running on ``calendar``.

        This will be used to infer a domain for pipelines that only use generic
        datasets when running in the context of a TradingAlgorithm.
        """
        return _DEFAULT_DOMAINS.get(calendar.name, domain.GENERIC)

    ##################
    # End Pipeline API
    ##################

    # def get_simulation_dt(self) -> datetime.datetime:
    #     return self.simulation_dt

    def execute_order_cancellation_policy(self):
        self.blotter.execute_cancel_policy(SimulationEvent.SESSION_END)

    def calculate_minute_capital_changes(self, dt: datetime.datetime):
        # process any capital changes that came between the last
        # and current minutes
        return self.calculate_capital_changes(dt, emission_rate=self.metrics_tracker.emission_rate,
                                              is_interday=False)

    # TODO: simplify
    # flake8: noqa: C901
    async def every_bar(
            self,
            dt_to_use: datetime.datetime,
            current_data: BarData,
            handle_data,
    ):
        # print(f"dt_to_use: in every_bar: {dt_to_use}")
        for capital_change in self.calculate_minute_capital_changes(dt_to_use):
            yield capital_change

        self.simulation_dt = dt_to_use
        # self.datetime = dt_to_use
        # called every tick (minute or day).
        # self.on_dt_changed(dt=dt_to_use)

        # handle any transactions and commissions coming out new orders
        # placed in the last bar
        new_transactions = []
        new_commissions = []
        closed_orders = []
        for exchange in self.exchanges.values():
            (
                new_trans,
                new_comm,
                closed,
            ) = await exchange.get_transactions(
                orders=self.blotter.get_open_orders(exchange_name=exchange.name),
                current_dt=self.simulation_dt
            )
            new_transactions.extend(new_trans)
            new_commissions.extend(new_comm)
            closed_orders.extend(closed)
        # print(f"getting transactions for {current_data.current_dt}, new transactions: {len(new_transactions)}, new commissions: {len(new_commissions)}, closed orders: {len(closed_orders)}" )
        self.blotter.prune_orders(closed_orders=closed_orders)

        for transaction in new_transactions:
            self._ledger.process_transaction(transaction=transaction)

            if transaction.order_id is None:
                # TODO: fix this when we get back order id in transaction
                continue

            # since this order was modified, record it
            order = self.blotter.get_order_by_id(transaction.order_id, exchange_name=transaction.exchange_name)
            self._ledger.process_order(order=order)

        for commission in new_commissions:
            self._ledger.process_commission(commission=commission)

        await handle_data(context=self, data=current_data, dt=dt_to_use)

        # grab any new orders from the blotter, then clear the list.
        # this includes cancelled orders.
        new_orders = self.new_orders
        self.new_orders = dict()

        # if we have any new orders, record them so that we know
        # in what perf period they were placed.
        for new_order in new_orders.values():
            self._ledger.process_order(order=new_order)

    def once_a_day(
            self,
            midnight_dt,
            current_data,
            asset_service,
    ):
        # process any capital changes that came overnight
        for capital_change in self.calculate_capital_changes(
                midnight_dt, emission_rate=self.metrics_tracker.emission_rate,
                is_interday=True
        ):
            yield capital_change

        # set all the timestamps
        self.simulation_dt = midnight_dt
        # self.datetime = midnight_dt
        # self.on_dt_changed(midnight_dt)

        self.metrics_tracker.handle_market_open(session_label=midnight_dt)

        # handle any splits that impact any positions or any open orders.
        assets_we_care_about = (
                self._ledger.position_tracker.positions.keys() | self.blotter.get_all_assets_in_open_orders()
        )

        if assets_we_care_about:
            splits = asset_service.get_splits(assets_we_care_about, midnight_dt)
            if splits:
                self.blotter.process_splits(splits)
                self._ledger.process_splits(splits)

    def on_exit(self):
        # Remove references to algo, data portal, et al to break cycles
        # and ensure deterministic cleanup of these objects when the
        # simulation finishes.
        self.benchmark_source = self.current_data = None

    async def transform(self):
        """
        Main generator work loop.
        """

        async with (AsyncExitStack() as stack):
            stack.callback(self.on_exit)
            stack.enter_context(ZiplineAPI(algo_instance=self))

            # if self.data_frequency < datetime.timedelta(days=1):
            #
            #     def execute_order_cancellation_policy():
            #         self.blotter.execute_cancel_policy(SimulationEvent.SESSION_END)
            #
            #     def calculate_minute_capital_changes(dt: datetime.datetime):
            #         # process any capital changes that came between the last
            #         # and current minutes
            #         return self.calculate_capital_changes(dt, emission_rate=emission_rate, is_interday=False)
            #
            # elif self.data_frequency == datetime.timedelta(days=1):
            #
            #     def execute_order_cancellation_policy():
            #         self.blotter.execute_daily_cancel_policy(SimulationEvent.SESSION_END)
            #
            #     def calculate_minute_capital_changes(dt: datetime.datetime):
            #         return []
            #
            # else:
            #
            #     def execute_order_cancellation_policy():
            #         pass
            #
            #     def calculate_minute_capital_changes(dt: datetime.datetime):
            #         return []
            errors = []
            for dt, action in self.clock:
                try:
                    if action == SimulationEvent.BAR:
                        async for capital_change_packet in self.every_bar(dt_to_use=dt, current_data=self.current_data,
                                                                          handle_data=self.event_manager.handle_data):
                            yield capital_change_packet, []
                    elif action == SimulationEvent.SESSION_START:
                        for capital_change_packet in self.once_a_day(midnight_dt=dt,
                                                                     current_data=self.current_data,
                                                                     asset_service=self.asset_service):
                            yield capital_change_packet, []
                    elif action == SimulationEvent.SESSION_END:
                        # End of the session.
                        positions = self._ledger.position_tracker.positions
                        position_assets = list(positions.keys())

                        # await self.asset_service.retrieve_all(
                        #     sids=[a.sid for a in positions]
                        # )

                        self._cleanup_expired_assets(dt=dt, position_assets=position_assets)

                        self.execute_order_cancellation_policy()
                        self.validate_account_controls()

                        yield self._get_daily_message(dt=dt), []
                    elif action == SimulationEvent.BEFORE_TRADING_START_BAR:
                        self.simulation_dt = dt
                        # self.datetime = dt
                        # self.on_dt_changed(dt=dt)
                        self.before_trading_start(data=self.current_data)
                    elif action == SimulationEvent.EMISSION_RATE_END and self.clock.emission_rate == datetime.timedelta(
                            minutes=1):
                        minute_msg = self._get_minute_message(
                            dt=dt,
                        )

                        yield minute_msg, []
                except Exception as e:
                    errors.append(
                        BarSimulationError(
                            trace=traceback.format_exc(),
                            message=f"Exception raised during simulation dt={dt}: {e}",
                            simulation_dt=dt
                        )
                    )
                    self._logger.error(f"Simulation error on dt={dt}")
                    if self.stop_on_error:
                        raise
            risk_message = self.metrics_tracker.handle_simulation_end()
            yield risk_message, errors

    def _cleanup_expired_assets(self, dt: datetime.datetime, position_assets):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """

        def past_auto_close_date(asset: Asset):
            acd = asset.auto_close_date
            if acd is not None:
                acd = acd
            return acd is not None and acd <= dt.date()

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [
            asset
            for asset in position_assets
            if past_auto_close_date(asset)
        ]
        # data_portal = self.data_portal
        for asset in assets_to_clear:
            self._ledger.close_position(asset=asset, dt=dt)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        assets_to_cancel = [
            asset
            for asset in self.blotter.get_all_assets_in_open_orders()
            if past_auto_close_date(asset=asset)
        ]

        for asset in assets_to_cancel:
            self.cancel_all_orders_for_asset(asset=asset)

        # Make a copy here so that we are not modifying the list that is being
        # iterated over.
        new_order_values = list(self.new_orders.values())
        # print(self.new_orders)
        for order in new_order_values:
            if order.status == OrderStatus.CANCELLED:
                self._ledger.process_order(order=order)
                self.new_orders.pop(order.id)

    def _get_daily_message(self, dt: datetime.datetime):
        """
        Get a perf message for the given datetime.
        """
        perf_message = self.metrics_tracker.handle_market_close(
            dt=dt,
        )
        perf_message["daily_perf"]["recorded_vars"] = self.recorded_vars
        return perf_message

    def _get_minute_message(self, dt: datetime.datetime):
        """
        Get a perf message for the given datetime.
        """
        rvars = self.recorded_vars

        minute_message = self.metrics_tracker.handle_minute_close(
            dt=dt,
        )

        minute_message["minute_perf"]["recorded_vars"] = rvars
        return minute_message


# Map from calendar name to default domain for that calendar.
_DEFAULT_DOMAINS = {d.calendar_name: d for d in domain.BUILT_IN_DOMAINS}
