from ziplime.finance.margin import FuturesMarginModel
import datetime
import sys

import structlog
from typing import AsyncIterator
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.services.asset_service import AssetService
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.data_source import DataSource
from ziplime.domain import account
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.controls.max_leverage import MaxLeverage

from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.pipeline.loaders import EquityPricingLoader
from ziplime.sources.benchmark_source import BenchmarkSource
from ziplime.trading.trading_algorithm_execution_status import TradingAlgorithmExecutionStatus
from ziplime.trading.trading_algorithm_executor import TradingAlgorithmExecutor
import polars as pl
from typing import Literal

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False

from ziplime.pipeline.data.equity_pricing import EquityPricing

from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
from ziplime.assets.entities.asset import Asset
from exchange_calendars import ExchangeCalendar
from ziplime.exchanges.repositories.exchange_repository import ExchangeRepository
from ziplime.assets.entities.exchange_asset import ExchangeAsset

logger = structlog.get_logger(__name__)


async def run_algorithm(
        algorithm: AlgorithmFile,
        asset_service: AssetService,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        clock: TradingClock,
        custom_data_sources: list[DataSource],
        exchange_repository: ExchangeRepository,
        stop_on_error: bool = False,
        benchmark_asset_symbol: str | None = None,
        benchmark_asset_mic: str | None = None,
        benchmark_returns: pl.Series | None = None,
        max_leverage: float = 1.0,
        same_bar_execution: bool = True,
        futures_margin_model: FuturesMarginModel | None = None,
        price_used_in_order_execution: Literal["open", "close", "low", "high"] = "close"
) -> TradingAlgorithmExecutionResult:
    """Run a backtest for the given algorithm.
    This is shared between the cli and :func:`ziplime.run_algo`.
    """
    tr = await _prepare_algorithm(algorithm=algorithm, asset_service=asset_service, print_algo=print_algo,
                                  metrics_set=metrics_set, custom_loader=custom_loader,
                                  clock=clock, custom_data_sources=custom_data_sources, stop_on_error=stop_on_error,
                                  benchmark_asset_symbol=benchmark_asset_symbol,
                                  benchmark_asset_mic=benchmark_asset_mic,
                                  benchmark_returns=benchmark_returns,
                                  max_leverage=max_leverage, same_bar_execution=same_bar_execution,
                                  futures_margin_model=futures_margin_model,
                                  exchange_repository=exchange_repository,
                                  price_used_in_order_execution=price_used_in_order_execution)
    trading_algorithm_executor = TradingAlgorithmExecutor()
    start_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    result = await trading_algorithm_executor.run_algorithm(trading_algorithm=tr)

    end_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    logger.info(
        f"Backtest completed in {int((end_time - start_time).total_seconds())} seconds. Errors: {len(result.errors)}")
    return result


async def run_algorithm_iter(
        algorithm: AlgorithmFile,
        asset_service: AssetService,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        clock: TradingClock,
        custom_data_sources: list[DataSource],
        exchange_repository: ExchangeRepository,
        stop_on_error: bool = False,
        benchmark_asset_symbol: str | None = None,
        benchmark_returns: pl.Series | None = None,
        max_leverage: float = 1.0,
        same_bar_execution: bool = True,
        futures_margin_model: FuturesMarginModel | None = None,
        price_used_in_order_execution: Literal["open", "close", "low", "high"] = "close"
) -> AsyncIterator[TradingAlgorithmExecutionStatus]:
    """Run a backtest for the given algorithm.
    This is shared between the cli and :func:`ziplime.run_algo`.
    """
    tr = await _prepare_algorithm(algorithm=algorithm, asset_service=asset_service, print_algo=print_algo,
                                  metrics_set=metrics_set, custom_loader=custom_loader,
                                  clock=clock, custom_data_sources=custom_data_sources, stop_on_error=stop_on_error,
                                  benchmark_asset_symbol=benchmark_asset_symbol, benchmark_returns=benchmark_returns,
                                  max_leverage=max_leverage, same_bar_execution=same_bar_execution,
                                  futures_margin_model=futures_margin_model,
                                  price_used_in_order_execution=price_used_in_order_execution,
                                  exchange_repository=exchange_repository)
    trading_algorithm_executor = TradingAlgorithmExecutor()
    start_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    async for status in trading_algorithm_executor.run_algorithm_iter(trading_algorithm=tr):
        yield status
    end_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    logger.info(
        f"Backtest completed in {int((end_time - start_time).total_seconds())} seconds.")


async def _prepare_algorithm(
        algorithm: AlgorithmFile,
        asset_service: AssetService,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        clock: TradingClock,
        custom_data_sources: list[DataSource],
        exchange_repository: ExchangeRepository,
        stop_on_error: bool = False,
        benchmark_asset_symbol: str | None = None,
        benchmark_asset_mic: str | None = None,
        benchmark_returns: pl.Series | None = None,
        max_leverage: float = 1.0,
        same_bar_execution: bool = True,
        futures_margin_model: FuturesMarginModel | None = None,
        price_used_in_order_execution: Literal["open", "close", "low", "high"] = "close"
) -> TradingAlgorithmExecutionResult:
    """Run a backtest for the given algorithm.
    This is shared between the cli and :func:`ziplime.run_algo`.
    """

    if print_algo:

        if PYGMENTS:
            highlight(
                algorithm.algorithm_text,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            logger.info(f"\n{algorithm.algorithm_text}")
    # exchanges_dict = {exchange.name: exchange for exchange in exchanges}
    pipeline_loader = EquityPricingLoader.without_fx(data_source=None,
                                                     asset_service=asset_service
                                                     )  # TODO: fix pipeline

    def choose_loader(column):
        if column in EquityPricing.columns:
            return pipeline_loader
        try:
            return custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)

    benchmark_asset = None
    if benchmark_asset_symbol is not None:
        if "@" in benchmark_asset_symbol:
            symbol, mic = benchmark_asset_symbol.split("@")
        else:
            symbol = benchmark_asset_symbol
            mic = benchmark_asset_mic
        benchmark_asset = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(
                symbol=symbol,
                mic=mic
            ),
            asset_type=AssetType.EQUITY
        )
        if benchmark_asset is None:
            raise ValueError(f"No asset found with symbol {benchmark_asset_symbol} for benchmark")
    benchmark_exchange = await exchange_repository.get_default_exchange()

    if len(clock.sessions) == 0:
        benchmark_precalculated_series = pl.Series()

    elif benchmark_asset is not None:
        benchmark_precalculated_series = await _initialize_precalculated_series(
            asset=benchmark_asset, trading_calendar=clock.trading_calendar, trading_days=clock.sessions,
            exchange=benchmark_exchange,
            benchmark_fields=frozenset({"close"}),
            sessions=clock.sessions,
            emission_rate=clock.emission_rate
        )
    elif benchmark_returns is not None:
        all_bars = pl.from_pandas(
            clock.trading_calendar.sessions_minutes(start=clock.sessions[0], end=clock.sessions[-1]).tz_convert(
                clock.trading_calendar.tz)
        )
        benchmark_precalculated_series = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
            index_column="date", every=clock.emission_rate
        ).agg(pl.col("close").sum())
    else:
        all_bars = pl.from_pandas(
            clock.trading_calendar.sessions_minutes(start=clock.sessions[0], end=clock.sessions[-1]).tz_convert(
                clock.trading_calendar.tz)
        )
        benchmark_precalculated_series = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
            index_column="date", every=clock.emission_rate
        ).agg(pl.col("close").sum())

    benchmark_source = BenchmarkSource(
        asset_service=asset_service,
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=clock.trading_calendar,
        sessions=clock.sessions,
        exchange=benchmark_exchange,
        emission_rate=clock.emission_rate,
        benchmark_fields=frozenset({"close"}),
        precalculated_series=benchmark_precalculated_series
    )
    if benchmark_asset is not None:
        # Running without a benchmark is supported -- the zero-returns series above is built for
        # exactly that case -- but validation used to run anyway and fail on the missing asset.
        await benchmark_source.validate_benchmark(benchmark_asset=benchmark_asset)

    for exchange in reversed(await exchange_repository.get_all_exchanges()):
        custom_data_sources.insert(0, exchange)

    tr = TradingAlgorithm(
        exchange_repository=exchange_repository,
        asset_service=asset_service,
        get_pipeline_loader=choose_loader,
        metrics_set=metrics_set,
        blotter=InMemoryBlotter(exchanges=await exchange_repository.get_all_exchanges(), cancel_policy=None),
        benchmark_source=benchmark_source,
        algorithm=algorithm,
        clock=clock,
        stop_on_error=stop_on_error,
        custom_data_sources=custom_data_sources,
        same_bar_execution=same_bar_execution,
        futures_margin_model=futures_margin_model,
    )

    orders_by_exchange = {}
    for exchange in await exchange_repository.get_all_exchanges():
        # exchange_orders = await exchange.get_orders()
        # trades = await exchange._trades(
        #     date_from=datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(hours=48),
        #     date_to=datetime.datetime.now(tz=datetime.timezone.utc),
        #     account_id=str(exchange.account_id)
        # )
        # order_ids = [trade.order_id for trade in trades.trades]
        # orders_by_exchange[exchange.name] = exchange_orders
        # # orders_by_ids = await exchange.get_orders_by_ids(order_ids=order_ids)
        # for order in exchange_orders.values():
        #     tr.new_order_submitted(order=order)
        # positions = await exchange.get_positions()
        portfolio = await  exchange.get_portfolio()
        tr._ledger.synchronize_exchange_portfolio(portfolio=portfolio)

    if max_leverage is not None:
        max_leverage = MaxLeverage(max_leverage, fail_on_error=False)
        tr.register_account_control(control=max_leverage)

    return tr


async def _initialize_precalculated_series(
        asset: ExchangeAsset, trading_calendar: ExchangeCalendar, trading_days: pl.Series,
        exchange: Exchange,
        emission_rate: datetime.timedelta,
        sessions: pl.Series,
        benchmark_fields: frozenset[str],
):
    """
    Internal method that pre-calculates the benchmark return series for
    use in the simulation.

    Parameters
    ----------
    asset:  Asset to use

    trading_calendar: TradingCalendar

    trading_days: pd.DateTimeIndex

    exchange: Exchange

    Notes
    -----
    If the benchmark asset started trading after the simulation start,
    or finished trading before the simulation end, exceptions are raised.

    If the benchmark asset started trading the same day as the simulation
    start, the first available minute price on that day is used instead
    of the previous close.

    We use history to get an adjusted price history for each day's close,
    as of the look-back date (the last day of the simulation).  Prices are
    fully adjusted for dividends, splits, and mergers.

    Returns
    -------
    returns : pd.Series
        indexed by trading day, whose values represent the %
        change from close to close.
    daily_returns : pd.Series
        the partial daily returns for each minute
    """
    all_bars: pl.Series = pl.from_pandas(
        trading_calendar.sessions_minutes(start=sessions[0], end=sessions[-1]).tz_convert(
            trading_calendar.tz)
    )
    limit = all_bars.to_frame("date").group_by_dynamic(
        index_column="date", every=emission_rate
    ).agg()["date"].len()

    benchmark_series = await exchange.get_data_by_limit(
        fields=benchmark_fields,
        limit=limit,
        frequency=emission_rate,
        end_date=all_bars[-1],
        assets=frozenset({asset}),
        include_end_date=True
    )
    return benchmark_series.with_columns(pl.col(benchmark_fields).pct_change().alias("pct_change"))  # [1:]
