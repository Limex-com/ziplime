import datetime
import importlib.util
import inspect
import sys
import traceback
import uuid
from collections import namedtuple, OrderedDict
from contextlib import AsyncExitStack
from copy import copy
import warnings
from typing import Callable, Literal
import pandas as pd
import structlog
from pygments.styles import default

import ziplime
from itertools import chain, repeat

from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.options.chain import OptionChain
from ziplime.finance.options.greeks import Greeks, greeks, position_greeks, time_to_expiry
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.logging_event import LoggingEvent
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.data_source import DataSource
from ziplime.domain.bar_data import BarData
from ziplime.exchanges.repositories.exchange_repository import ExchangeRepository
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
from ziplime.finance.controls.max_leverage import MaxLeverage
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.exchanges.exchange import Exchange
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
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

from ziplime.finance.execution import ExecutionStyle, make_execution_style
from ziplime.finance.asset_restrictions import Restrictions
from ziplime.finance.cancel_policy import CancelPolicy
from ziplime.finance.asset_restrictions import (
    NoRestrictions,
)
from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.finance.margin import FuturesMarginModel
from ziplime.assets.entities.bond import Bond
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
from ziplime.vectorized.signals import (
    PricePanel, SignalPanel, as_of_panel, normalise_signals, verify_causality,
)
import polars as pl


# For creating and storing pipeline instances
AttachedPipeline = namedtuple("AttachedPipeline", "pipe chunks eager")


class NoBenchmark(ValueError):
    def __init__(self):
        super(NoBenchmark, self).__init__(
            "Must specify either benchmark_sid or benchmark_returns.",
        )


def _named_universe(universe) -> dict:
    """Accept either shape a strategy already writes its universe in.

    A dict names the panel's columns explicitly. A plain list -- which is what every strategy in
    `examples/huggingface` already builds -- is named by ticker, so vectorising one of those is a
    `compute_signals` function and nothing else.
    """
    if isinstance(universe, dict):
        return universe
    if not isinstance(universe, (list, tuple, set, frozenset)):
        raise TypeError(
            f"context.universe has to be a list of listings or a dict of name -> listing, got "
            f"{type(universe).__name__}.")

    named = {}
    for listing in universe:
        symbol = getattr(listing, "symbol", None)
        if symbol is None:
            raise TypeError(
                f"context.universe holds a {type(listing).__name__}, which has no symbol to name "
                f"a column by. Pass listings, or a dict of name -> listing.")
        if symbol in named:
            # Two venues, one ticker. Naming both "T" would silently drop one of them from the
            # panel, so the strategy has to say which name means which listing.
            raise ValueError(
                f"Two listings in context.universe are both called {symbol!r} "
                f"({named[symbol].mic} and {listing.mic}). Give them names: "
                f"context.universe = {{'{symbol}.{named[symbol].mic}': ..., "
                f"'{symbol}.{listing.mic}': ...}}.")
        named[symbol] = listing
    return named


def _as_datetime(value, tz, default: datetime.date) -> datetime.datetime:
    """A source's start bound as an aware datetime, however it was stored."""
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=tz)
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min, tzinfo=tz)
    return datetime.datetime.combine(default, datetime.time.min, tzinfo=tz)


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
            exchange_repository: ExchangeRepository,
            # Algorithm API
            metrics_set,
            blotter: Blotter,
            benchmark_source: BenchmarkSource | None,
            clock: TradingClock,
            custom_data_sources: list[DataSource],
            capital_changes=None,
            get_pipeline_loader=None,
            create_event_context=None,
            stop_on_error: bool = False,
            same_bar_execution: bool = True,
            futures_margin_model: FuturesMarginModel | None = None,
            intraday_metrics: bool = False,

    ):
        self.algorithm = algorithm
        self.config = algorithm.config
        self.exchange_repository = exchange_repository
        self.stop_on_error = stop_on_error
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

        self._ledger = Ledger(futures_margin_model=futures_margin_model,
                              trading_sessions=clock.sessions,
                              data_frequency=clock.emission_rate)

        self._initialize = algorithm.initialize
        self._handle_data = algorithm.handle_data
        self._before_trading_start = algorithm.before_trading_start
        # Optional analyze function, gets called after run
        self._analyze = algorithm.analyze
        # The vectorised half, computed once between `initialize` and the first bar.
        self._compute_signals = getattr(algorithm, "compute_signals", None)
        self._signals_warmup = int(getattr(algorithm, "warmup", 0) or 0)
        #: Set by :meth:`_compute_vectorised_signals`; read by strategies as `context.signals`.
        self.signals = None

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
            data_sources=data_sources,
            data_source_resolver=self._resolve_named_data_source,
        )

        # We don't have a datetime for the current snapshot until we
        # receive a message.
        self.simulation_dt = None

        self.clock = clock

        self.same_bar_execution = same_bar_execution
        #: See MetricsTracker: the whole metric set on every intraday bar, or only at the close.
        self.intraday_metrics = intraday_metrics
        self._logger = structlog.get_logger(__name__)
        self._session_count = 0
        #: Listings whose bars ran out mid-run, see `data_delistings`.
        self._data_delistings: dict[ExchangeAsset, datetime.date] = {}
        if self.same_bar_execution:
            self._logger.warning(
                "LOOK-AHEAD: same-bar execution is on. Orders submitted from handle_data fill in "
                "the SAME bar, so a decision taken on that bar's close is filled at that same "
                "close -- a price the market had not printed when the decision was made. Results "
                "are optimistic by roughly one bar of edge. Pass same_bar_execution=False to fill "
                "on the next bar instead.")
        else:
            self._logger.warning(
                "You are NOT running same day execution. Submitted orders in handle_data will be executed in the NEXT bar after handle_data is finished.")

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

    async def before_trading_start(self, data):
        """Run the algorithm's daily preparation hook.

        Awaits the hook when it is `async def`. It is the one place zipline expects the day's data
        to be read, and every read in this fork -- `data.history`, `data.current`,
        `huggingface_dataset` -- is a coroutine, so an author writing the natural thing got a
        coroutine that was built, dropped and never run. No exception, no warning in the output,
        and a hook that silently did nothing all backtest.
        """
        self.compute_eager_pipelines()

        if self._before_trading_start is None:
            return

        self._in_before_trading_start = True

        # `before_trading_start` fires 46 minutes before the open, which is not a market minute at
        # any intraday rate -- not only at one minute. Testing for equality left every other
        # intraday rate reading data at a minute the calendar does not have.
        with handle_non_market_minutes(
                data
        ) if self.clock.emission_rate < datetime.timedelta(days=1) else ExitStack():
            result = self._before_trading_start(self, data)
            if inspect.isawaitable(result):
                await result

        self._in_before_trading_start = False

    async def handle_data(self, data):
        if self._handle_data:
            await self._handle_data(self, data)

    # def analyze(self, perf):
    #     if self._analyze is None:
    #         return
    #
    #     with ZiplineAPI(self):
    #         self._analyze(self, perf)

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
        exchanges_dict = {exchange.name: exchange for exchange in await self.exchange_repository.get_all_exchanges()}
        self.metrics_tracker = MetricsTracker(
            asset_service=self.asset_service,
            exchanges=exchanges_dict,
            trading_calendar=self.clock.trading_calendar,
            sessions=self.clock.sessions,
            first_session=self.clock.start_session,
            last_session=self.clock.end_session,
            emission_rate=self.clock.emission_rate,
            ledger=self._ledger,
            metrics=self._metrics_set,
            benchmark_source=self.benchmark_source,
            intraday_metrics=self.intraday_metrics,
        )

        # Set the dt initially to the period start by forcing it to change.
        # self.on_dt_changed(dt=self.clock.start_session)
        # self.datetime =self.clock.start_session
        self.simulation_dt = self.clock.start_session
        if not self.initialized:
            for exchange in await self.exchange_repository.get_all_exchanges():
                start_cash_balance = exchange.get_start_cash_balance()
                if start_cash_balance <= 0:
                    raise ZeroCapitalError()
            await self.initialize()
            self.initialized = True
            assets = getattr(self, "assets", None)
            if assets:
                await self.asset_service.preload_corporate_actions(
                    assets=list({asset.asset for asset in assets}),
                    date_from=self.clock.start_session,
                    date_to=self.clock.end_session,
                )
        await self._compute_vectorised_signals()
        await self.metrics_tracker.handle_start_of_simulation()
        return self.transform()

    async def _compute_vectorised_signals(self) -> None:
        """Run the strategy's ``compute_signals`` once, before the first bar.

        This is the whole of the in-run hybrid. The indicators are array work and are done here in
        one pass; the decisions stay in `handle_data`, bar by bar, with the ordinary blotter,
        slippage and commissions. A strategy that does not define ``compute_signals`` is untouched
        by any of it.

        The panel handed to the hook covers the run plus exactly ``WARMUP`` bars before it -- not
        however much history the bundle happens to hold, because then a strategy's first signals
        would depend on how deeply the bundle was ingested rather than on what it declared.
        """
        if self._compute_signals is None:
            return

        universe = getattr(self, "universe", None)
        if not universe:
            raise ValueError(
                "compute_signals needs to know which instruments to compute over. Set "
                "`context.universe = {'JNJ': listing, ...}` in initialize -- the keys become the "
                "column names of the price panel and of the signals read back in handle_data.")
        universe = _named_universe(universe)

        calendar = self.clock.trading_calendar
        sessions = list(self.clock.sessions)
        emission = self.clock.emission_rate
        # Generous on purpose: fetching too little is a correctness bug, fetching too much costs
        # a moment and is trimmed to the declared warm-up below.
        bars_per_session = (1 if emission >= datetime.timedelta(days=1)
                            else max(1, int(datetime.timedelta(hours=24) / emission)))
        limit = (len(sessions) + self._signals_warmup + 1) * bars_per_session

        # In the calendar's own zone, not the UTC `session_close` returns: bundles are stamped that
        # way, and polars refuses to compare two zones rather than quietly aligning them.
        end_stamp = calendar.session_close(self.clock.end_session).tz_convert(calendar.tz)

        source = await self.current_data.resolve_data_source(None)
        rows = await source.get_data_by_limit(
            fields=None, limit=limit, frequency=emission, include_end_date=True,
            end_date=end_stamp.to_pydatetime(), assets=frozenset(universe.values()))
        if rows.is_empty():
            raise ValueError(
                f"No bars for {', '.join(universe)} in the run's window, so there is nothing to "
                f"compute signals over.")

        names = {listing.sid: name for name, listing in universe.items()}
        panel = PricePanel.from_bundle_rows(rows, names)
        panel = self._trim_to_warmup(panel, first_session=self.clock.start_session)
        panel = await self._attach_datasets(panel, universe=universe, names=names)

        computed = normalise_signals(self._compute_signals(self, panel), panel.index)
        verify_causality(lambda prices: self._compute_signals(self, prices),
                         panel=panel, computed=computed, warmup=self._signals_warmup)
        self.signals = SignalPanel(computed, clock=self.get_datetime)

        in_run = sum(1 for stamp in panel.index if stamp.date() >= self.clock.start_session)
        expected = len(sessions) * bars_per_session
        if emission >= datetime.timedelta(days=1) and in_run < expected:
            self._logger.warning(
                "The price panel is short of the run's sessions; on a bar it has no row for, a "
                "signal reads its previous value, the way a forward-filled price does",
                sessions=expected, panel_rows=in_run)
        self._logger.info("Computed signals vectorised, ahead of the run",
                          signals=sorted(computed), instruments=len(universe),
                          bars=len(panel), warmup=self._signals_warmup)

    async def _attach_datasets(self, panel: PricePanel, universe: dict, names: dict) -> PricePanel:
        """Mount whatever `context.datasets` declares and resolve each as of every bar.

        This is what lets a vectorised signal read fundamentals, filings or disclosures. Those
        arrive a few times a year rather than once a bar, so each is resolved into the same shape
        the prices already have -- at every bar, what was known by then -- by
        :func:`~ziplime.vectorized.signals.as_of_panel`, which answers the question `data.current`
        answers at one bar for the whole history at once.

        The resolution the source declares is carried across rather than guessed: a source that
        republishes revisions coalesces by column, and reading it by row instead silently drops
        most of what it holds.
        """
        declared = getattr(self, "datasets", None)
        if not declared:
            return panel
        if not isinstance(declared, dict):
            raise TypeError(
                f"context.datasets has to be a dict of name -> source, got "
                f"{type(declared).__name__}.")

        calendar = self.clock.trading_calendar
        end_stamp = calendar.session_close(self.clock.end_session).tz_convert(calendar.tz)

        datasets = {}
        for label, address in declared.items():
            source = await self.current_data.resolve_data_source(address)
            # Everything the source holds, not just the run's window. WARMUP governs how much
            # price history an indicator gets; an as-of view needs something different and
            # unbounded -- the statement current on the first bar is whichever one was filed last
            # before it, which may be eleven months earlier, and a window that starts at the first
            # bar simply does not contain it. Fetching from the source's own start covers it.
            reach = end_stamp.to_pydatetime() - _as_datetime(
                getattr(source, "start_date", None), calendar.tz, default=datetime.date(1900, 1, 1))
            rows = await source.get_data_by_window(
                fields=None, since=reach + datetime.timedelta(days=1),
                end_date=end_stamp.to_pydatetime(), frequency=self.clock.emission_rate,
                assets=frozenset(universe.values()), include_end_date=True)
            if rows.is_empty():
                self._logger.warning(
                    "Dataset has no rows over the run's window, so every signal built from it "
                    "will be empty", dataset=label, source=getattr(source, "name", str(source)))
            coalesce = str(getattr(getattr(source, "resolution", None), "value", "")) == "coalesce"
            datasets[label] = as_of_panel(rows, names=names, index=panel.index,
                                          coalesce=coalesce)
            self._logger.info("Resolved a dataset as of every bar", dataset=label,
                              rows=len(rows), coalesce=coalesce, fields=datasets[label].fields)

        return PricePanel({field: getattr(panel, field) for field in panel.fields},
                          panel.index, datasets)

    def _trim_to_warmup(self, panel: PricePanel, first_session: datetime.date) -> PricePanel:
        """Keep the run's bars plus exactly the declared warm-up ahead of them."""
        positions = [ix for ix, stamp in enumerate(panel.index) if stamp.date() >= first_session]
        if not positions:
            raise ValueError(
                f"The bundle has no bars on or after {first_session}, the run's first session.")
        if positions[0] < self._signals_warmup:
            # Worth saying out loud: an indicator that never fills its window produces NaN, and a
            # comparison against NaN is False rather than an error, so the strategy simply does
            # not trade early and nothing in the output says why.
            self._logger.warning(
                "The bundle holds less history before the run than WARMUP asks for, so indicators "
                "start the run part-way through their windows",
                warmup_requested=self._signals_warmup, warmup_available=positions[0],
                first_session=str(first_session))
        start = max(0, positions[0] - self._signals_warmup)
        if start == 0:
            return panel
        kept = {field: getattr(panel, field).iloc[start:] for field in panel.fields}
        return PricePanel(kept, panel.index[start:])

    def data_delistings(self) -> dict[ExchangeAsset, datetime.date]:
        """Instruments the run held whose bars simply stopped, and the session they stopped on.

        Only the unannounced ones. A bond that redeems and a futures contract that expires both
        say so in their own reference data and leave the book through that; what lands here is the
        case nothing declares -- an equity delisting, or a bundle that was ingested short. Both
        look identical from inside the engine and both are worth saying out loud, because the
        window after them contributes sessions of flat, riskless return to every metric computed
        over the run.
        """
        return dict(self._data_delistings)

    def _record_data_delisting(self, asset: ExchangeAsset, session: datetime.date) -> None:
        if asset in self._data_delistings:
            return
        self._data_delistings[asset] = self.current_data.last_bar_session(asset)
        self._logger.warning(
            "No bars after this session; the position is closed at its last mark and the "
            "listing cannot be traded again",
            symbol=asset.symbol, mic=asset.mic,
            last_bar=str(self._data_delistings[asset]), dt=str(session))

    async def calculate_capital_changes(
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

        # Both of these are coroutines and both were called without awaiting, so a target capital
        # change was computed against unsynced prices and then applied to a portfolio that had not
        # been updated. Nothing raised; the deposit was simply the wrong size.
        await self._sync_last_sale_prices()
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
        await self._ledger.capital_change(change_amount=capital_change_amount)

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
            if self.clock.emission_rate < datetime.timedelta(days=1)
            else
            # A daily simulation has one bar per session, so there is no time of day to schedule
            # against and the time rule is ignored. Any intraday rate does have one: this used to
            # test for exactly one minute, which silently discarded `market_open(minutes=30)` on a
            # five-minute run and fired the function on every bar instead.
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
    def futures_margin_requirement(self, maintenance: bool = False,
                                   currency: str | None = None) -> float:
        """Margin the open futures positions tie up, in one currency.

        Pass ``currency`` when the book spans venues that collect different ones -- an exchange
        may collect
        roubles even for its dollar-quoted contracts, CME collects dollars. Returns 0.0 when
        margin is not being modelled; check :meth:`models_futures_margin` to tell that apart from
        a book that genuinely needs no margin.
        """
        return self._ledger.futures_margin_requirement(maintenance=maintenance, currency=currency)

    @api_method
    def futures_margin_by_currency(self, maintenance: bool = False) -> dict:
        """Margin posted per currency, e.g. ``{"RUB": 41_000.0, "USD": 10_500.0}``."""
        return self._ledger.futures_margin_by_currency(maintenance=maintenance)

    @api_method
    def models_futures_margin(self) -> bool:
        """Whether this simulation models futures margin at all."""
        return self._ledger.futures_margin_model.models_margin

    @api_method
    def realism_warnings(self) -> list:
        """Effects this simulation does not reproduce, given how it was configured.

        See :mod:`ziplime.finance.realism`.
        """
        from ziplime.finance.realism import realism_warnings
        return realism_warnings(
            futures_margin_model=self._ledger.futures_margin_model,
            same_bar_execution=self.same_bar_execution,
            trades_futures=any(
                isinstance(position.asset.asset, FuturesContract)
                for position in self._ledger.position_tracker.get_position_list()
            ) or True,
            trades_bonds=any(
                isinstance(position.asset.asset, Bond)
                for position in self._ledger.position_tracker.get_position_list()
            ),
        )

    @api_method
    def notional_exposure(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        """Notional of ``amount`` contracts at ``price``: ``amount * price * multiplier``.

        Futures sizing is a notional calculation, so this is the number to size against rather
        than ``amount * price``.
        """
        multiplier = getattr(asset.asset, "multiplier", 1.0)
        return amount * price * multiplier

    @api_method
    def contracts_for_notional(self, asset: ExchangeAsset, notional: float,
                               price: float) -> int:
        """Whole contracts closest to ``notional`` of exposure, rounded toward zero."""
        multiplier = getattr(asset.asset, "multiplier", 1.0)
        if price == 0 or multiplier == 0:
            return 0
        return int(notional / (price * multiplier))

    @api_method
    async def continuous_future(
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
        return await self.asset_service.create_continuous_future(
            root_symbol=root_symbol_str,
            offset=offset,
            roll_style=roll,
            adjustment=adjustment,
        )

    @api_method
    async def symbol(
            self,
            symbol: str,
            mic: str = None,
            asset_type: AssetType = AssetType.EQUITY,
            # exchange_name: str = None,
            country_code: str | None = None
    ) -> ExchangeAsset | None:
        """Lookup an Equity by its ticker symbol.

        Parameters
        ----------
        symbol : str
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
        if "@" in symbol:
            symbol, mic = symbol.split("@")
        # else:
        #     symbol = symbol
        #     if mic is None:
        #         raise ValueError("You must supply MIC or use symbol in format ticker@MIC")

        asset = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=symbol, mic=mic),
            asset_type=asset_type,
        )
        if asset is None:
            raise SymbolNotFound(symbol=symbol)
        return asset

    @api_method
    async def symbols(self, *args, **kwargs):
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
        # `symbol` is a coroutine, so the list comprehension used to return a list of coroutines
        # rather than a list of listings -- and nothing raised until the caller tried to order one.
        return [await self.symbol(identifier, **kwargs) for identifier in args]

    @api_method
    async def symbols_universe(self, name: str, dt: datetime.date = None):
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
        return await self.asset_service.get_symbols_universe(name=name, dt=dt or self.simulation_dt)

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
    async def bond_symbol(self, symbol: str, mic: str = None) -> ExchangeAsset | None:
        """Look up a bond listing by ticker, in ``TICKER`` or ``TICKER@MIC`` form.

        A shorthand for ``symbol(..., asset_type=AssetType.BOND)``. Worth having its own name:
        the same ticker can be an equity on one venue and a bond on another, and asking for the
        wrong type is the kind of mistake that shows up as a strange price rather than an error.
        """
        return await self.symbol(symbol=symbol, mic=mic, asset_type=AssetType.BOND)

    @api_method
    async def bond_schedule(self, asset: ExchangeAsset) -> list:
        """Every stored coupon, amortization, maturity and offer event of ``asset``, by date."""
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return self._ledger.bond_book.events(bond)

    @api_method
    async def accrued_interest(self, asset: ExchangeAsset, dt: datetime.date = None) -> float:
        """Coupon accrued on one bond -- what a buyer owes the seller on top of the quote."""
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return self._ledger.bond_book.accrued_interest(bond, dt or self.simulation_dt)

    @api_method
    async def bond_face_value(self, asset: ExchangeAsset, dt: datetime.date = None) -> float:
        """Principal outstanding on one bond, after every amortization instalment paid so far."""
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return self._ledger.bond_book.face_value(bond, dt or self.simulation_dt)

    @api_method
    async def bond_dirty_price(self, asset: ExchangeAsset, quoted_price: float,
                               dt: datetime.date = None) -> float:
        """Money one bond changes hands for at ``quoted_price``: clean value plus accrued interest."""
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return self._ledger.bond_book.dirty_value(bond, quoted_price, dt or self.simulation_dt)

    @api_method
    async def bond_current_yield(self, asset: ExchangeAsset, quoted_price: float,
                                 dt: datetime.date = None) -> float:
        """Annual coupon income as a fraction of what the bond costs to buy today."""
        from ziplime.finance.bonds import current_yield
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return current_yield(bond, self._ledger.bond_book, quoted_price,
                             dt or self.simulation_dt)

    @api_method
    async def bond_yield_to_maturity(self, asset: ExchangeAsset, quoted_price: float,
                                     dt: datetime.date = None) -> float:
        """Simple (non-compounded) annualised return of holding to maturity from ``quoted_price``."""
        from ziplime.finance.bonds import simple_yield_to_maturity
        bond = self._require_bond(asset)
        await self._ledger.bond_book.load(self.asset_service, [bond])
        return simple_yield_to_maturity(bond, self._ledger.bond_book, quoted_price,
                                        dt or self.simulation_dt)

    @staticmethod
    def _require_bond(asset: ExchangeAsset) -> Bond:
        """Unwrap the bond behind a listing, with a message that names what was passed instead."""
        instrument = getattr(asset, "asset", asset)
        if not isinstance(instrument, Bond):
            raise TypeError(
                f"Expected a bond listing, got {type(instrument).__name__}. Look the instrument "
                f"up with `await context.bond_symbol(...)`."
            )
        return instrument

    @api_method
    async def futures_symbol(self, symbol: str, mic: str = None) -> ExchangeAsset | None:
        """Look up a futures **listing** by ticker, in ``TICKER`` or ``TICKER@MIC`` form.

        This is what you order. :meth:`future_symbol` returns the contract behind a listing, which
        carries the specification but is not tradeable — passing it to :meth:`order` fails, because
        an order needs the listing that owns the sid.
        """
        return await self.symbol(symbol=symbol, mic=mic,
                                 asset_type=AssetType.FUTURES_CONTRACT)

    async def _resolve_named_data_source(self, name: str):
        """Mount a data source a strategy named but never registered.

        Today that means a Hugging Face dataset address. It is resolved here rather than in
        :class:`~ziplime.domain.bar_data.BarData` because a mount needs the asset database, to
        turn the dataset's tickers into sids, and the simulation window, to avoid downloading
        years the run cannot reach -- and this object holds both.
        """
        from ziplime.data.data_sources.huggingface.huggingface_data_source import (
            HuggingFaceDataSource, is_address,
        )
        if not is_address(name):
            raise KeyError(
                f"No data source named {name!r}. Register it with "
                f"run_simulation(custom_data_sources=[...]), or name a Hugging Face dataset as "
                f"hf://owner/name/config.")
        return await HuggingFaceDataSource.mount(
            name, asset_service=self.asset_service,
            start_date=self.clock.start_session, end_date=self.clock.end_session,
            session_timezone=str(self.clock.trading_calendar.tz))

    @api_method
    async def huggingface_dataset(self, repo_id: str, config: str | None = None,
                                  revision: str | None = None,
                                  fields: list[str] | None = None,
                                  start_date: datetime.date | None = None,
                                  end_date: datetime.date | None = None,
                                  name: str | None = None):
        """Mount a point-in-time dataset from the Hugging Face Hub.

        The explicit form of ``data.history(data_source="hf://owner/name/config")``. Use it when
        the defaults are not what you want -- above all to **pin a revision**, so a result can be
        reproduced after the dataset has grown:

            context.congress = await context.huggingface_dataset(
                "ZipLime/congress-trading", config="features", revision="67c335f5")

        Then read it like any other source::

            df = await data.history(assets=[apple], bar_count=30,
                                    data_source=context.congress)

        Args:
            repo_id: ``owner/name`` on the Hub, or a full ``hf://owner/name/config`` address.
            config: Table to mount. Defaults to the dataset's ``features`` table if it has one.
            revision: Branch, tag or commit. Defaults to the default branch, resolved to the
                commit it points at now and reported, so the run can be repeated exactly.
            fields: Columns to keep besides ``date`` and ``sid``. All of them by default.
            start_date, end_date: Window to fetch. Defaults to the simulation's own, which is
                what keeps a long dataset from being downloaded in full.
            name: What to call the source. Defaults to its address.

        Returns:
            The mounted source. Nothing is downloaded until it is first read.
        """
        from ziplime.data.data_sources.huggingface.huggingface_data_source import (
            HuggingFaceDataSource,
        )
        source = await HuggingFaceDataSource.mount(
            repo_id, config=config, revision=revision, asset_service=self.asset_service,
            start_date=start_date or self.clock.start_session,
            end_date=end_date or self.clock.end_session,
            fields=fields, name=name,
            session_timezone=str(self.clock.trading_calendar.tz))
        self.current_data.data_sources[source.name] = source
        return source

    @api_method
    async def futures_chain(self, root_symbol: str, mic: str = None) -> list[ExchangeAsset]:
        """The contract chain of ``root_symbol`` as **listings**, ordered by expiration.

        This is the term structure: element 0 is the front contract, and each one after it is
        further out on the curve. Ordering by expiration rather than by ticker matters -- an
        alphabetical sort puts ``CLF27`` before ``CLX26``, which reverses the curve.

        Returns the listings, so the result can be passed straight to :meth:`order` and to
        ``data.current``; :attr:`ExchangeAsset.asset` on each one carries the multiplier and the
        expiration date.
        """
        return await self.asset_service.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol, mic=mic)

    @api_method
    async def option_chain(self, underlying: ExchangeAsset | str,
                           expiration_date: datetime.date | None = None,
                           mic: str | None = None) -> OptionChain:
        """The option chain on ``underlying``, as a selectable
        :class:`~ziplime.finance.options.chain.OptionChain`.

        ``expiration_date`` defaults to **today's session**, which is what makes this the 0DTE
        call: the chain listed this morning, expiring at this afternoon's close. Pass a date to
        reach another expiry.

        A 0DTE strategy has to call this every session. The contracts are listed fresh each day and
        every symbol in the chain changes with it -- ``SPY240614C00523000`` exists for one session
        and never again -- so there is nothing to resolve once in ``initialize`` and hold onto.

        Raises:
            ValueError: if no contracts are listed for that underlying and expiry. On a 0DTE chain
                that usually means the session has no chain rather than that the underlying is
                wrong.
        """
        symbol = underlying.symbol if isinstance(underlying, ExchangeAsset) else underlying
        if mic is None and isinstance(underlying, ExchangeAsset):
            mic = underlying.mic
        session = expiration_date or self.get_datetime().date()
        listings = await self.asset_service.get_exchange_option_contracts(
            underlying_symbol=symbol, expiration_date=session, mic=mic)
        if not listings:
            raise ValueError(
                f"No options listed on {symbol} expiring {session}. For a 0DTE chain that is a "
                f"statement about the session, not about the underlying: the chain exists only on "
                f"its own expiration day.")
        return OptionChain.from_listings(listings, expiration_date=session)

    @api_method
    async def option_symbol(self, symbol: str, mic: str | None = None) -> ExchangeAsset | None:
        """Resolve one option listing by its OCC symbol, e.g. ``SPY240614C00523000``."""
        return await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=symbol, mic=mic), asset_type=AssetType.OPTIONS_CONTRACT)

    @api_method
    def time_to_expiry(self, asset: ExchangeAsset) -> float:
        """Years left on ``asset``, measured to its expiration session's **close**.

        The number every option formula takes, and on a 0DTE contract the one that moves fastest:
        at the open it is about 0.0018 years and ninety minutes before the close it is a tenth of
        that. Measuring in whole days instead -- the obvious shortcut -- overprices an afternoon
        straddle several-fold.

        Returns 0.0 once the closing bar is reached, which is not an error: it is the instant the
        contract settles at intrinsic value.
        """
        contract = asset.asset
        if not isinstance(contract, OptionContract):
            raise TypeError(f"{asset.symbol} is not an option listing.")
        calendar = self.clock.trading_calendar
        expires_at = calendar.session_close(contract.expiration_date).tz_convert(
            calendar.tz).to_pydatetime()
        return time_to_expiry(self.get_datetime(), expires_at)

    @api_method
    async def option_greeks(self, asset: ExchangeAsset, volatility: float | None = None,
                            rate: float = 0.0, amount: float = 0.0) -> Greeks:
        """Price and Greeks for one option listing at the current bar.

        ``volatility`` defaults to the ``implied_volatility`` column of the contract's own current
        bar when the data source carries one -- the synthetic feed does, and a real one should --
        and raises when it does not, rather than substituting a number nobody chose.

        With ``amount`` the figures are scaled to a position of that many contracts (multiplier
        included), so ``amount=-10`` on a short call gives the delta of the book rather than of one
        unit.
        """
        contract = asset.asset
        if not isinstance(contract, OptionContract):
            raise TypeError(f"{asset.symbol} is not an option listing.")
        underlying = contract.underlying_exchange_asset
        if underlying is None:
            raise ValueError(
                f"{asset.symbol} has no underlying listing stored, so its Greeks cannot be "
                f"computed: every one of them needs the underlying's price.")

        spot = await self._spot_price(asset=underlying, dt=self.get_datetime())
        if spot is None:
            raise ValueError(f"No price for {underlying.symbol} at {self.get_datetime()}.")

        if volatility is None:
            volatility = await self._implied_volatility_of(asset)
            if volatility is None:
                raise ValueError(
                    f"The data source carries no implied volatility for {asset.symbol}, so pass "
                    f"volatility= explicitly. Inferring one from the price is possible with "
                    f"ziplime.finance.options.greeks.implied_volatility, but it should be a "
                    f"decision the strategy makes rather than a default.")

        per_unit = greeks(contract.option_type, spot, contract.strike,
                          self.time_to_expiry(asset), rate, volatility)
        if amount:
            return position_greeks(per_unit, amount=amount, multiplier=contract.multiplier)
        return per_unit

    async def _implied_volatility_of(self, asset: ExchangeAsset) -> float | None:
        """The ``implied_volatility`` of this contract's current bar, if the source carries one."""
        return await self._read_field(asset=asset, dt=self.get_datetime(),
                                      field="implied_volatility")

    @api_method
    async def future_symbol(self, symbol: str, mic: str = None) -> FuturesContract | None:
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
        return await self.asset_service.get_futures_contract_by_symbol(symbol=symbol, mic=mic)

    async def _calculate_order_value_amount(self, asset: ExchangeAsset, value: float, exchange: Exchange):
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
            last_price_data = (await exchange.get_spot_value(frozenset({asset}),
                                                             dt=self.simulation_dt,
                                                             fields=frozenset({"price"}),
                                                             data_frequency=None
                                                             ))["price"]
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
        if type(asset.asset) is FuturesContract:
            return value / (last_price * asset.asset.multiplier)
        elif type(asset.asset) is Bond:
            # A bond quote is a percentage of face value, and the buyer also pays accrued
            # interest, so the money one bond costs is neither of those numbers on its own.
            await self._ledger.bond_book.load(self.asset_service, [asset.asset])
            per_bond = self._ledger.bond_price_in_money(asset=asset, quoted_price=last_price,
                                                        dt=self.simulation_dt)
            if per_bond == 0:
                return 0
            return value / per_bond
        else:
            return value / last_price

    def _can_order_asset(self, asset: ExchangeAsset):
        day = self.clock.trading_calendar.minute_to_session(self.simulation_dt).date()

        # Out of bars. Reference data does not record an equity's delisting -- the listing keeps
        # an end date decades away -- so without this an order in a dead name is accepted and
        # filled at the last price it ever printed, however many months ago that was.
        if self.current_data.has_stopped_trading(asset=asset, session=day):
            self._record_data_delisting(asset=asset, session=day)
            return False

        if asset.auto_close_date:
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
    async def order(self, asset: ExchangeAsset, amount: int, style: ExecutionStyle,
                    account_id: str | None = None,
                    exchange_name: str | None = None) -> Order | None:
        """Place an order for a fixed number of shares.

        Parameters
        ----------
        asset : ExchangeAsset
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
        if isinstance(asset, ContinuousFuture):
            raise ValueError(
                f"Cannot place an order for {asset}: a continuous future is a data specifier, "
                f"not a tradeable contract. Order the contract it currently resolves to, which "
                f"`await data.current_contract(continuous_future)` returns."
            )
        if not self._can_order_asset(asset=asset):
            return None
        if isinstance(asset.asset, Bond):
            # The ledger settles the fill synchronously and needs the coupon schedule to price it
            # dirty, so it is fetched here, while we are still on an async path.
            await self._ledger.bond_book.load(self.asset_service, [asset.asset])
        # TODO: implement dynamic risk control

        await self.validate_order_params(asset=asset, amount=amount)
        if exchange_name is None:
            exchange = await self.exchange_repository.get_default_exchange()
            exchange_name = exchange.name
        else:
            exchange = await self.exchange_repository.get_exchange_by_mic(mic=exchange_name)
        order_id = uuid.uuid4().hex[:20]

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
            exchange_name=exchange.name,
            trading_account_id=exchange.account_id
        )
        if amount == 0:
            self._logger.warning("Not executing order for zero shares.")
            return None

        # quote_asset = await self.asset_service.get_currency_by_symbol(symbol="USD",
        #                                                               exchange_name=exchange_name)

        self._logger.info(LoggingEvent.ORDER_SUBMIT, style=str(style), quantity=order_qty_rounded,
                          asset_symbol=asset.symbol, asset_mic=asset.mic,
                          simulation_dt=self.simulation_dt)

        submitted_order = await exchange.submit_order(order=order)
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

    async def validate_order_params(self, asset: ExchangeAsset, amount: int):
        """Check an order against every registered trading control before it is placed.

        Every control's ``validate`` is ``async def`` -- `MaxPositionSize` reads the current price
        to check a notional cap -- and this method used to be synchronous and call them without
        awaiting. Each call built a coroutine, dropped it, and returned None, so
        `set_max_position_size`, `set_max_order_size`, `set_max_order_count`, `set_long_only` and
        `set_asset_restrictions` all accepted their arguments and then enforced nothing. These are
        the fail-safes; a fail-safe that silently does not fire is worse than none.

        Raises:
            TradingControlViolation: if a control with ``on_error="fail"`` rejects the order.
        """

        if not self.initialized:
            raise OrderDuringInitialize(
                msg="order() can only be called from within handle_data()"
            )

        for control in self.trading_controls:
            await control.validate(
                asset=asset,
                amount=amount,
                portfolio=self.portfolio,
                algo_datetime=self.simulation_dt,
                algo_current_data=self.current_data,
            )

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_value(self, asset: ExchangeAsset, value: float, limit_price: float | None = None,
                          stop_price: float | None = None,
                          style: ExecutionStyle | None = None,
                          exchange_name: str | None = None
                          ):
        """Place an order for a fixed amount of money.

        Equivalent to ``order(asset, value / data.current(asset, 'price'))``.

        Parameters
        ----------
        asset : ExchangeAsset
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
        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange()
        )
        amount = await self._calculate_order_value_amount(asset=asset, value=value, exchange=exchange)
        return await self.order(
            asset,
            amount,
            # `order` takes an execution style, not loose prices: passing limit_price/stop_price
            # through raised a TypeError, so order_value only ever worked by accident when the
            # caller supplied a style of its own.
            style=make_execution_style(limit_price=limit_price, stop_price=stop_price,
                                       style=style),
            exchange_name=exchange_name
        )

    @property
    def recorded_vars(self):
        return copy(self._recorded_vars)

    async def _sync_last_sale_prices(self, dt: datetime.datetime = None):
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
            # await self._ledger.sync_last_sale_prices(dt=dt, handle_non_market_minutes=False)
            await self.sync_last_sale_prices_to_ledger(dt=dt,
                                                       handle_non_market_minutes=False)  # TODO : remove

            self._last_sync_time = dt

    async def sync_last_sale_prices_to_ledger(self, dt: datetime.datetime,
                                              handle_non_market_minutes: bool = False):
        # exchange = self.exchanges[exchange_name]
        assets = [(position.asset, position.exchange_name) for position in self._ledger.positions]
        if not assets:
            return
        if handle_non_market_minutes:
            previous_minute = exchange.trading_calendar.previous_minute(minute=dt)
            prices = exchange.get_adjusted_value(
                field="close",
                dt=previous_minute,
                perspective_dt=dt,
                # frequency=self.data_frequency
            )

        else:
            price_by_asset = {}
            for exchange_name, exchange_positions in self._ledger.position_tracker.positions_by_exchange.items():
                exchange = await self.exchange_repository.get_exchange_by_mic(mic=exchange_name[0])
                assets = [position.asset for position in exchange_positions]

                chunk = await exchange.get_spot_value(
                    fields=frozenset(["close"]),
                    dt=dt,
                    assets=frozenset(assets),
                    # data_frequency=self.data_frequency
                )
                price_by_asset.update({
                    (row["sid"], exchange): row["close"]
                    for row in chunk.select(["sid", "close"]).to_dicts()
                })

        self._ledger.sync_last_sale_prices(dt=dt, prices=price_by_asset)

    @property
    def portfolio(self):
        # self._sync_last_sale_prices()
        return self._ledger.portfolio

    @property
    def account(self):
        # self._sync_last_sale_prices()
        return self._ledger.account

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
    def set_slippage(self, us_equities=None, us_futures=None, bonds=None):
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

        if bonds is not None:
            if Bond not in bonds.allowed_asset_types:
                raise IncompatibleSlippageModel(
                    asset_type="bonds",
                    given_model=bonds,
                    supported_asset_types=bonds.allowed_asset_types,
                )
            self.blotter.slippage_models[Bond] = bonds

    @api_method
    def set_commission(self, us_equities=None, us_futures=None, bonds=None):
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

        if bonds is not None:
            if Bond not in bonds.allowed_asset_types:
                raise IncompatibleCommissionModel(
                    asset_type="bonds",
                    given_model=bonds,
                    supported_asset_types=bonds.allowed_asset_types,
                )
            self.blotter.commission_models[Bond] = bonds

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
            self, asset: ExchangeAsset, percent: float, style: ExecutionStyle,
            exchange_name: str | None = None
    ):
        """Place an order in the specified asset corresponding to the given
        percent of the current portfolio value.

        Parameters
        ----------
        asset : ExchangeAsset
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
        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange()
        )

        amount = await self._calculate_order_percent_amount(asset=asset, percent=percent, exchange=exchange)
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    async def _calculate_order_percent_amount(self, asset: ExchangeAsset, percent: float, exchange: Exchange,
                                              reserved_percentage_for_fees: float = 0.00):
        value = self.portfolio.portfolio_value * percent

        requested_quantity = await self._calculate_order_value_amount(asset=asset, value=value,
                                                                      exchange=exchange)
        commission = exchange.get_commission_model(asset=asset)
        slippage = exchange.get_slippage_model(asset=asset)
        projected_commission = commission.calculate_for_asset(asset=asset, quantity=requested_quantity,
                                                              transaction_amount=value)
        new_quantity = await self._calculate_order_value_amount(asset=asset, value=value - projected_commission,
                                                                exchange=exchange)
        self._logger.info(
            f"Projected commission for {requested_quantity} quantity of {asset.symbol} is {projected_commission}. Quantity corrected to {new_quantity}"
        )
        # return new_quantity
        estimated_price, estimated_quantity = await slippage.order_target_percentage_maximum_quantity(asset=asset,
                                                                                                      exchange=exchange,
                                                                                                      percentage=percent,
                                                                                                      available_cash=value - projected_commission,
                                                                                                      dt=self.simulation_dt)
        # print(
        #     f"[{self.simulation_dt}] Calculating PRICE FOR handle_data ({estimated_quantity} * {estimated_price})={estimated_quantity * estimated_price}, price_with_slippage={estimated_price} cash_before={self.portfolio.cash}")
        return min(estimated_quantity, new_quantity)

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target(
            self, asset: ExchangeAsset, target: int, style: ExecutionStyle,
            exchange_name: str | None = None
    ):
        """Place an order to adjust a position to a target number of shares. If
        the position doesn't already exist, this is equivalent to placing a new
        order. If the position does exist, this is equivalent to placing an
        order for the difference between the target number of shares and the
        current number of shares.

        Parameters
        ----------
        asset : ExchangeAsset
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
        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange()
        )

        amount = self._calculate_order_target_amount(
            asset=asset,
            target=target,
            exchange=exchange,
            trading_account_id=exchange.account_id
        )
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    def _calculate_order_target_amount(self, exchange: Exchange, trading_account_id: str, asset: ExchangeAsset,
                                       target: int):
        current_position = self.portfolio.positions.get((exchange.name,trading_account_id, asset), None)
        if current_position is not None:
            # current_position = self.portfolio.positions[asset].amount
            target -= current_position.amount

        return target

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target_value(
            self, asset: ExchangeAsset, target: float, style: ExecutionStyle,
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
        asset : ExchangeAsset
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

        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange()
        )

        target_amount = await self._calculate_order_value_amount(asset=asset, value=target, exchange=exchange)
        amount = self._calculate_order_target_amount(
            asset=asset,
            target=target_amount,
            exchange=exchange,
            trading_account_id=exchange.account_id
        )
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

    @api_method
    @disallowed_in_before_trading_start(OrderInBeforeTradingStart())
    async def order_target_percent(
            self, asset: ExchangeAsset, target: float,
            style: ExecutionStyle, exchange_name: str | None = None,
            reserved_percentage_for_fees: float = 0.05
    ):
        """Place an order to adjust a position to a target percent of the
        current portfolio value. If the position doesn't already exist, this is
        equivalent to placing a new order. If the position does exist, this is
        equivalent to placing an order for the difference between the target
        percent and the current percent.

        Parameters
        ----------
        asset : ExchangeAsset
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

        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange())

        target_amount = await self._calculate_order_percent_amount(asset=asset, percent=target,
                                                                   exchange=exchange,
                                                                   reserved_percentage_for_fees=reserved_percentage_for_fees)
        amount = self._calculate_order_target_amount(
            asset=asset,
            target=target_amount,
            exchange=exchange,
            trading_account_id=exchange.account_id
        )
        return await self.order(
            asset=asset,
            amount=amount,
            style=style,
            exchange_name=exchange_name
        )

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
        # `blotter.open_orders` is keyed by exchange name first and by listing second. Indexing it
        # with a listing therefore matched nothing -- an asset is never an exchange name -- so this
        # returned [] for every asset that had orders working, and the no-asset form returned
        # exchange names mapped to listings rather than orders. A strategy topping an order up
        # relies on this to see what is already working; without it, it stacks order on order.
        if asset is None:
            merged: dict[ExchangeAsset, list[Order]] = {}
            for exchange_orders in self.blotter.open_orders.values():
                for listing, orders in exchange_orders.items():
                    if orders:
                        merged.setdefault(listing, []).extend(orders.values())
            return merged

        open_orders: list[Order] = []
        for exchange_name in self.blotter.open_orders:
            orders = self.blotter.get_open_orders_by_asset(asset=asset,
                                                           exchange_name=exchange_name)
            if orders:
                open_orders.extend(orders.values())
        return open_orders

    @api_method
    def get_order(self, order_id: str, exchange_name: str) -> Order | None:
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
        # await self.exchanges[exchange_name].get_orders_by_ids([order_id])
        return self.blotter.get_order_by_id(order_id=order_id, exchange_name=exchange_name)

    @api_method
    async def cancel_order(self, order_id: str, exchange_name: str, relay_status: bool = True) -> None:
        """Cancel an open order.

        Parameters
        ----------
        order_param : str or Order
            The order_id or order object to cancel.
        """
        order = self.blotter.get_order_by_id(order_id=order_id, exchange_name=exchange_name)
        if order is None or not order.open:
            return
        order.cancel()
        order.dt = self.simulation_dt
        # we want this order's new status to be relayed out
        # along with newly placed orders.

        self.blotter.order_cancelled(order=order)
        exchange = await self.exchange_repository.get_exchange_by_mic(exchange_name) if exchange_name else (
            await self.exchange_repository.get_default_exchange()
        )
        await exchange.cancel_order(order_id=order.exchange_order_id)
        if relay_status:
            self.new_orders[order.id] = order
        else:
            self.new_orders.pop(order.id, None)

    async def cancel_all_orders_for_asset(self, asset: ExchangeAsset, exchange_name: str, warn: bool = False,
                                          relay_status: bool = True):
        """
        Cancel all open orders for a given asset.
        """
        # (sadly) open_orders is a defaultdict, so this will always succeed.
        orders = self.blotter.get_open_orders_by_asset(asset=asset, exchange_name=exchange_name)
        if not orders:
            return
        # The comment below has been here since ziplime forked, and it is right about the problem
        # and wrong about the code: cancelling *does* mutate the blotter's open orders, and this
        # loop iterated the live mapping rather than a copy, so it raised `RuntimeError: dictionary
        # changed size during iteration` the moment it had anything to cancel. It is reached when
        # an order is still open on a contract that has expired -- routine on an option book, and
        # essentially never on an equity one, which is why it survived this long.
        for order_id, order in list(orders.items()):
            await self.cancel_order(order_id=order.id, exchange_name=order.exchange_name, relay_status=relay_status)
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
        self.blotter.cancel_all_orders_for_asset(asset=asset, exchange_name=exchange_name, relay_status=relay_status)

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

    async def validate_account_controls(self):
        for control in self.account_controls:
            await control.validate(
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
        return domain.GENERIC

    ##################
    # End Pipeline API
    ##################

    # def get_simulation_dt(self) -> datetime.datetime:
    #     return self.simulation_dt

    def execute_order_cancellation_policy(self):
        self.blotter.execute_cancel_policy(SimulationEvent.SESSION_END)

    def calculate_minute_capital_changes(self, dt: datetime.datetime):
        # process any capital changes that came between the last
        # and current minutes. An async generator: iterate it with `async for`.
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
        async for capital_change in self.calculate_minute_capital_changes(dt_to_use):
            yield capital_change

        self.simulation_dt = dt_to_use
        # self.datetime = dt_to_use
        # called every tick (minute or day).
        # self.on_dt_changed(dt=dt_to_use)
        if self.same_bar_execution:
            await handle_data(context=self, data=current_data, dt=dt_to_use)

        # handle any transactions and commissions coming out new orders
        # placed in the last bar
        new_transactions = []
        new_commissions = []
        closed_orders = []
        for exchange in await self.exchange_repository.get_all_exchanges():
            # print("LEVERAGE BEFORE: ", self.account.leverage, self.account.net_leverage)

            (
                new_trans,
                new_comm,
                closed,
            ) = await exchange.get_transactions(
                orders=self.blotter.get_open_orders(exchange_name=exchange.name),
                current_dt=self.simulation_dt,
                same_bar_execution=self.same_bar_execution,
            )
            new_transactions.extend(new_trans)
            new_commissions.extend(new_comm)
            closed_orders.extend(closed)
            # print("LEVERAGE: AFTER ", self.account.leverage, self.account.net_leverage)

        # print(f"getting transactions for {current_data.current_dt}, new transactions: {len(new_transactions)}, new commissions: {len(new_commissions)}, closed orders: {len(closed_orders)}" )
        self.blotter.prune_orders(closed_orders=closed_orders)

        for transaction in new_transactions:
            self._ledger.process_transaction(transaction=transaction)
            # if self.account.leverage > 2:
            #    print("a")
            # print("LEVERAGE: AFTER 2", self.account.leverage, self.account.net_leverage)

            if transaction.order_id is None:
                # TODO: fix this when we get back order id in transaction
                continue

            # since this order was modified, record it
            order = self.blotter.get_order_by_id(transaction.order_id, exchange_name=transaction.exchange_name)
            self._ledger.process_order(order=order)
            # print("LEVERAGE: AFTER 3", self.account.leverage, self.account.net_leverage)

        # print("LEVERAGE: BEFORE COMMISION", self.account.leverage, self.account.net_leverage)

        for commission in new_commissions:
            self._ledger.process_commission(commission=commission, tr=self)
        # print("LEVERAGE: BEFORE 4", self.account.leverage, self.account.net_leverage)
        if not self.same_bar_execution:
            await handle_data(context=self, data=current_data, dt=dt_to_use)
        # print("LEVERAGE: AFTER 4", self.account.leverage, self.account.net_leverage)

        # grab any new orders from the blotter, then clear the list.
        # this includes cancelled orders.
        new_orders = self.new_orders
        # print(f"[{self.simulation_dt}]new_orders={new_orders}")
        self.new_orders = dict()

        # if we have any new orders, record them so that we know
        # in what perf period they were placed.
        for new_order in new_orders.values():
            self._ledger.process_order(order=new_order)
            # print("LEVERAGE: AFTER 5", self.account.leverage, self.account.net_leverage)

    async def once_a_day(
            self,
            midnight_dt,
            current_data,
            asset_service,
    ):
        # process any capital changes that came overnight
        async for capital_change in self.calculate_capital_changes(
                midnight_dt, emission_rate=self.metrics_tracker.emission_rate,
                is_interday=True
        ):
            yield capital_change

        # set all the timestamps
        self.simulation_dt = midnight_dt
        # self.datetime = midnight_dt
        # self.on_dt_changed(midnight_dt)

        # move processing of ledger and dividends before metrics
        self._ledger.start_of_session(session_label=midnight_dt)
        # TODO: handle ajustments repository
        # adjustment_reader = self.asset_service._adjustments_repository
        # if adjustment_reader is not None:
        # this is None when running with a dataframe source
        await self._ledger.process_dividends(
            next_session=midnight_dt,
            asset_service=self.asset_service,
        )
        # Coupons and amortization instalments first, then redemption: the final coupon of a bond
        # maturing today is earned while the position still exists.
        await self._ledger.process_bond_events(
            next_session=midnight_dt,
            asset_service=self.asset_service,
        )
        await self._ledger.redeem_matured_bonds(
            session=midnight_dt,
            asset_service=self.asset_service,
        )
        # self._sync_last_sale_prices(dt=datetime.datetime.combine(midnight_dt, datetime.time())) # my
        # self._ledger.update_portfolio()
        # self._ledger.update_account()

        await self.metrics_tracker.handle_market_open(session_label=midnight_dt)

        # handle any splits that impact any positions or any open orders.
        # assets_we_care_about = (
        #         self._ledger.position_tracker.positions.keys() | self.blotter.get_all_assets_in_open_orders()
        # )
        assets_we_care_about = set([pos.asset.asset for pos in self._ledger.position_tracker.get_position_list()] + [exchange_asset.asset for
                                                                                                exchange_asset in
                                                                                                self.blotter.get_all_assets_in_open_orders()])

        if assets_we_care_about:
            splits = await asset_service.get_splits(assets_we_care_about, midnight_dt)
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
                        await self._sync_last_sale_prices(dt=dt)
                        await self._ledger.update_portfolio()
                        self._ledger.update_account()

                        async for capital_change_packet in self.every_bar(dt_to_use=dt, current_data=self.current_data,
                                                                          handle_data=self.event_manager.handle_data):
                            yield capital_change_packet, []

                    elif action == SimulationEvent.SESSION_START:
                        # TODO: add also portfolio update
                        async for capital_change_packet in self.once_a_day(midnight_dt=dt,
                                                                           current_data=self.current_data,
                                                                           asset_service=self.asset_service):
                            yield capital_change_packet, []
                    elif action == SimulationEvent.SESSION_END:
                        # End of the session.
                        positions = self._ledger.position_tracker.positions
                        position_assets = [p.asset for p in self._ledger.position_tracker.get_position_list()]

                        # await self.asset_service.retrieve_all(
                        #     sids=[a.sid for a in positions]
                        # )
                        await self._sync_last_sale_prices(dt=dt)
                        await self._ledger.update_portfolio()
                        self._ledger.update_account()
                        await self._cleanup_expired_assets(dt=dt, position_assets=position_assets)

                        self.execute_order_cancellation_policy()
                        await self.validate_account_controls()

                        if self.clock.emission_rate == datetime.timedelta(days=1):
                            # this method is called for both minutely and daily emissions, but
                            # this chunk of code here only applies for daily emissions. (since
                            # it's done every minute, elsewhere, for minutely emission).
                            await self.sync_last_sale_prices_to_ledger(dt=dt,
                                                                       handle_non_market_minutes=False)  # TODO : remove
                            await self._sync_last_sale_prices(dt=dt)
                            await self._ledger.update_portfolio()
                            self._ledger.update_account()

                        session_ix = self._session_count
                        # increment the day counter before we move markers forward.
                        self._session_count += 1
                        self._ledger.end_of_session(session_ix=session_ix)

                        yield self._get_daily_message(dt=dt), []
                    elif action == SimulationEvent.BEFORE_TRADING_START_BAR:
                        self.simulation_dt = dt
                        # self.datetime = dt
                        # self.on_dt_changed(dt=dt)
                        await self.before_trading_start(data=self.current_data)
                    elif (action == SimulationEvent.EMISSION_RATE_END
                          and self.clock.emission_rate < datetime.timedelta(days=1)):
                        # Syncing sale prices to the ledger happens here for intraday rates and in
                        # the session-end branch for daily ones. Testing for exactly one minute
                        # meant a five-minute run did neither, and carried a portfolio value that
                        # never moved between sessions.
                        # await self._ledger.sync_last_sale_prices(dt=dt, handle_non_market_minutes=False)
                        await self.sync_last_sale_prices_to_ledger(dt=dt,
                                                                   handle_non_market_minutes=False)  # TODO : remove

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

            errors.extend(self._end_of_run_warnings())
            risk_message = self.metrics_tracker.handle_simulation_end()
            yield risk_message, errors

    def _end_of_run_warnings(self) -> list[BarSimulationError]:
        """Facts about the run that a table of metrics cannot show, reported where they are read.

        Two of them, both about a record that does not say what it looks like it says:

        * sessions the run never recorded, which makes every metric a metric over a shorter window
          than the one the run is labelled with;
        * instruments whose prices ran out mid-run, which is a delisting nothing else announces --
          they contribute flat, riskless sessions from then on.

        These go in ``errors`` rather than into a log line because that is what a caller reads:
        ``result.errors`` is checked, and the twentieth warning of a run is not.
        """
        warnings = []

        expected = len(self.clock.sessions)
        if self._session_count < expected:
            recorded = self.clock.sessions[self._session_count - 1] if self._session_count else None
            message = (
                f"Performance record covers {self._session_count} of {expected} sessions: it ends "
                f"at {recorded} instead of {self.clock.sessions[-1]}. Every metric in this result "
                f"is computed over that shorter window, not over the one that was asked for."
            )
            self._logger.error(message)
            warnings.append(BarSimulationError(trace="", message=message,
                                               simulation_dt=self.simulation_dt))

        if self._data_delistings:
            listed = ", ".join(
                f"{asset.symbol}@{asset.mic} after {session}"
                for asset, session in sorted(self._data_delistings.items(),
                                             key=lambda item: item[0].symbol))
            message = (
                f"The market data stops mid-run for {len(self._data_delistings)} listing(s) with "
                f"nothing in their reference data to explain it: {listed}. Each was closed at its "
                f"last mark and could not be traded again, so the sessions after it are flat for "
                f"that name -- check whether it delisted or the bundle is simply short."
            )
            self._logger.error(message)
            warnings.append(BarSimulationError(trace="", message=message,
                                               simulation_dt=self.simulation_dt))

        return warnings

    async def _settlement_price(self, asset: ExchangeAsset, dt: datetime.datetime) -> float | None:
        """What an expiring position settles at, or ``None`` to use its last mark.

        Only options answer with a price, and the answer is arithmetic rather than a quote: an
        expiring option is worth ``max(S - K, 0)`` per unit for a call and ``max(K - S, 0)`` for a
        put, against wherever the underlying finished. Closing at the last mark instead is wrong in
        both directions on the same day -- a wing that stopped being quoted at lunchtime carries a
        few cents into a settlement of zero, and the strike that finishes ten cents in the money
        carries nothing into ten dollars a contract.

        **Cash settlement only.** A physically settled contract is settled in cash at intrinsic and
        says so, rather than quietly delivering a hundred shares nobody asked for.
        """
        instrument = getattr(asset, "asset", None)
        if not isinstance(instrument, OptionContract):
            return None

        underlying = instrument.underlying_exchange_asset
        if underlying is None:
            self._logger.warning(
                "Expiring option has no underlying listing, so it cannot be settled at intrinsic "
                "value; closing at its last mark instead",
                symbol=asset.symbol, dt=str(dt))
            return None

        underlying_price = await self._spot_price(asset=underlying, dt=dt)
        if underlying_price is None:
            self._logger.warning(
                "No price for the underlying on the expiration session, so the option cannot be "
                "settled at intrinsic value; closing at its last mark instead",
                symbol=asset.symbol, underlying=underlying.symbol, dt=str(dt))
            return None

        if instrument.settlement_type.is_deliverable:
            self._logger.warning(
                "Settling a physically delivered option in cash at its intrinsic value; delivery "
                "of the underlying is not modelled",
                symbol=asset.symbol, dt=str(dt))
        return instrument.intrinsic_value(underlying_price)

    async def _spot_price(self, asset: ExchangeAsset, dt: datetime.datetime) -> float | None:
        """The close of ``asset``'s most recent bar at ``dt``, or ``None`` if it has none."""
        return await self._read_field(asset=asset, dt=dt, field="close")

    async def _read_field(self, asset: ExchangeAsset, dt: datetime.datetime,
                          field: str) -> float | None:
        """One column of ``asset``'s most recent bar at ``dt``.

        Reads through the default exchange rather than the asset's own venue. Those are different
        things and the repository's `get_exchange_by_mic` does not bridge them: it is keyed by the
        exchange's *name* -- the broker the simulation trades through, "LIME" -- while a listing's
        `mic` names where the instrument is listed, "ARCX". Asking it for a listing's MIC raises a
        KeyError on every simulation with one exchange, which is all of them.
        """
        exchange = await self.exchange_repository.get_default_exchange()
        rows = await exchange.get_spot_value(fields=frozenset([field]), dt=dt,
                                             assets=frozenset({asset}))
        if rows is None or rows.is_empty() or field not in rows.columns:
            return None
        value = rows[field][-1]
        return None if value is None else float(value)

    async def _cleanup_expired_assets(self, dt: datetime.datetime, position_assets):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """

        session = dt.date()

        def past_auto_close_date(asset: ExchangeAsset):
            acd = asset.auto_close_date
            return acd is not None and acd <= session

        def delisted(asset: ExchangeAsset):
            """Expired by the calendar, or out of bars.

            Two different facts with the same consequence. A futures contract announces its own
            end and carries an ``auto_close_date``; an equity does not -- a delisted listing keeps
            its row and an end date decades away, and the only record of the delisting is that the
            bars stop. Closing on the first fact alone left the second kind on the book at a mark
            that never moved again, still counted in exposure, leverage and returns.
            """
            return (past_auto_close_date(asset)
                    or self.current_data.has_stopped_trading(asset=asset, session=session))

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [
            asset
            for asset in position_assets
            if delisted(asset)
        ]
        # data_portal = self.data_portal
        for asset in assets_to_clear:
            if not past_auto_close_date(asset):
                self._record_data_delisting(asset=asset, session=session)
            self._ledger.close_position(
                asset=asset, dt=dt,
                price=await self._settlement_price(asset=asset, dt=dt))

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        assets_to_cancel = [
            asset
            for asset in self.blotter.get_all_assets_in_open_orders()
            if delisted(asset=asset)
        ]

        for asset in assets_to_cancel:
            for exchange in await self.exchange_repository.get_all_exchanges():
                await self.cancel_all_orders_for_asset(asset=asset, exchange_name=exchange.name)

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
