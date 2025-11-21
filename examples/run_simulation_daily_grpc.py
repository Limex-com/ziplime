import asyncio
import datetime
import logging
import pathlib

import polars as pl
import structlog

from ziplime.utils.logging_utils import configure_logging

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry

logger = structlog.get_logger(__name__)


async def _run_simulation():
    bundle_storage_path = str(Path(Path.home(), ".ziplime", "data"))
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    asset_service = get_asset_service(
        clear_asset_db=False,
        # db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    symbols = ["META@XNGS", "AAPL", "AMZN", "NFLX", "GOOGL", "VXX"]
    ny = pytz.timezone("America/New_York")
    start_local = datetime.datetime(2025, 9, 1, 0, 0)  # 2025-09-01 00:00 local clock time
    end_local = datetime.datetime(2025, 9, 17, 0, 0)  # 2025-09-01 00:00 local clock time

    start_date = ny.localize(start_local)  # Correct: EDT (UTC-04:00)
    end_date = ny.localize(end_local)
    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last()
    ]
    market_data_bundle = await bundle_service.load_bundle(bundle_name="grpc_daily_data",
                                                          bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=start_date,
                                                          end_date=end_date + datetime.timedelta(days=1),
                                                          symbols=symbols,
                                                          aggregations=aggregations
                                                          )


    # By default, SimulationExchange with LIME name is used

    # run daily simulation
    res, errors = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=[],
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="META",
        benchmark_returns=None,
        stop_on_error=False,
        asset_service=asset_service,
        default_exchange_name="XNGS"
    )
    if errors:
        logger.error(errors)
    print(res.head(n=10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())
