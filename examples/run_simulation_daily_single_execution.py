import asyncio
import datetime
import logging
import pathlib

import polars as pl
import structlog

from ziplime.gens.domain.single_execution_clock import SingleExecutionClock
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.logging_utils import configure_logging

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE

logger = structlog.get_logger(__name__)


async def _run_simulation():
    start_date = datetime.datetime(year=2025, month=1, day=3, tzinfo=pytz.timezone("America/New_York"))
    end_date = datetime.datetime(year=2025, month=2, day=1, tzinfo=pytz.timezone("America/New_York"))
    emission_rate = datetime.timedelta(days=1)
    bundle_service = get_bundle_service()

    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    # Use aggregations if you ingested data of frequnecy less than 1 day
    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last()
    ]
    market_data_bundle = await bundle_service.load_bundle(bundle_name="limex_us_minute_data",
                                                          bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=start_date,
                                                          end_date=end_date,
                                                          symbols=["META", "AAPL", "AMZN", "NFLX", "GOOGL",
                                                                   ],
                                                          start_auction_delta=datetime.timedelta(minutes=15),
                                                          end_auction_delta=datetime.timedelta(minutes=15),
                                                          aggregations=aggregations,
                                                          )

    custom_data_sources = []
    custom_data_sources.append(
        await bundle_service.load_bundle(bundle_name="limex_us_fundamental_data", bundle_version=None))

    equity_commission = PerShare(
        cost=DEFAULT_PER_SHARE_COST,
        min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,

    )
    clock = SingleExecutionClock(
        trading_calendar=get_calendar("NYSE"),
        start_date=start_date,
        end_date=end_date,
        emission_rate=emission_rate,
    )
    # run daily simulation
    res, errors = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=custom_data_sources,
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=emission_rate,
        benchmark_asset_symbol="AAPL",
        benchmark_returns=None,
        stop_on_error=True,
        asset_service=asset_service,
        equity_commission=equity_commission,
        clock=clock
    )

    if errors:
        logger.error(errors)
    print(res.head(n=10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())
