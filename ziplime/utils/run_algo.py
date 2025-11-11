import datetime
import sys

import structlog

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.services.asset_service import AssetService
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.data.services.data_source import DataSource
from ziplime.exchanges.exchange import Exchange

from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.sources.benchmark_source import BenchmarkSource
import polars as pl

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False

from ziplime.pipeline.data import USEquityPricing
from ziplime.pipeline.loaders import USEquityPricingLoader

from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)


async def run_algorithm(
        algorithm: AlgorithmFile,
        asset_service: AssetService,
        print_algo: bool,
        exchanges: list[Exchange],
        metrics_set: str,
        custom_loader,
        clock: TradingClock,
        custom_data_sources: list[DataSource],
        stop_on_error: bool = False,
        benchmark_asset_symbol: str | None = None,
        benchmark_returns: pl.Series | None = None,
):
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
    exchanges_dict = {exchange.name: exchange for exchange in exchanges}
    pipeline_loader = USEquityPricingLoader.without_fx(None)  # TODO: fix pipeline

    def choose_loader(column):
        if column in USEquityPricing.columns:
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
            mic = None
        benchmark_asset = await asset_service.get_asset_by_symbol(symbol=symbol,
                                                                  asset_type=AssetType.EQUITY,
                                                                  exchange_name=mic)
        if benchmark_asset is None:
            raise ValueError(f"No asset found with symbol {benchmark_asset_symbol} for benchmark")

    benchmark_source = BenchmarkSource(
        asset_service=asset_service,
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=clock.trading_calendar,
        sessions=clock.sessions,
        exchange=exchanges[0],
        emission_rate=clock.emission_rate,
        benchmark_fields=frozenset({"close"})
    )

    tr = TradingAlgorithm(
        exchanges=exchanges_dict,
        asset_service=asset_service,
        get_pipeline_loader=choose_loader,
        metrics_set=metrics_set,
        blotter=InMemoryBlotter(exchanges=exchanges_dict, cancel_policy=None),
        # benchmark_source=get_benchmark(clock=clock),
        benchmark_source=benchmark_source,
        #        benchmark_source=None,
        algorithm=algorithm,
        clock=clock,
        stop_on_error=stop_on_error,
        custom_data_sources=custom_data_sources
    )

    start_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    perf, errors = await tr.run()
    end_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    logger.info(f"Backtest completed in {int((end_time - start_time).total_seconds())} seconds. Errors: {len(errors)}")
    return perf, errors
