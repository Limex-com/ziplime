import asyncio
import datetime
import logging
from zoneinfo import ZoneInfo

import polars as pl
import structlog

from ziplime.assets.domain.asset_type import AssetType
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging
from ziplime.assets.entities.asset_symbol import AssetSymbol

from pathlib import Path

from examples.providers_config import ASSET_DB_PATH

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerDollar
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

logger = structlog.get_logger(__name__)

DAY = datetime.timedelta(days=1)
MAX_RATE_AGE = DAY * 7


async def _run_simulation():
    tz = ZoneInfo("America/New_York")
    start_date = datetime.datetime(year=2025, month=4, day=3, tzinfo=tz)
    end_date = datetime.datetime(year=2025, month=5, day=9, hour=23, minute=59, second=59, tzinfo=tz)
    bundle_service = get_bundle_service()

    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    # Use aggregations if you ingested data of frequnecy less than 1 day
    aggregations = [
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
        pl.col("symbol").last()
    ]
    symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]

    exchange_assets = await asset_service.get_exchange_assets_by_symbols(symbols=[AssetSymbol(
        symbol=symbol, mic=None
    ) for symbol in symbols], asset_type=AssetType.EQUITY)

    market_data_bundle, missing_data = await bundle_service.load_bundle(bundle_name="yahoo_finance_daily_data",
                                                                        bundle_version=None,
                                                                        frequency=datetime.timedelta(days=1),
                                                                        start_date=start_date,
                                                                        end_date=end_date,
                                                                        assets=exchange_assets,
                                                                        aggregations=aggregations,
                                                                        )

    definition, storage = await bundle_service.load_bundle_definition(bundle_name="yahoo_finance_daily_data")
    # Use the timestamps stored during ingestion. The CBR reference rates in this
    # bundle are effective on those dates and do not need a market-close delay.
    fx_listing = await asset_service.get_exchange_currency_by_symbol(
        symbol=AssetSymbol(symbol="EURUSD=X", mic="CCY"),
    )

    conversion = await CurrencyConversionService.from_bundle(
        storage=storage,
        bundle=definition,
        listings=[fx_listing],
        start=start_date,
        end=end_date,
        max_rate_age=MAX_RATE_AGE,
    )
    reporting = CurrencyReporting(
        base_currency="USD",
        currencies=("USD", "EUR"),
        conversion=conversion,
    )

    custom_data_sources = []
    equity_commission = PerDollar(
        cost=0.001,
    )
    equity_slippage = FixedBasisPointsSlippage()

    # run daily simulation
    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file=str(Path("../algorithms/test_algo/test_algo_yahoo_finance.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=custom_data_sources,
        config_file=str(Path("../algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="AAPL",
        benchmark_returns=None,
        stop_on_error=True,
        asset_service=asset_service,
        equity_commission=equity_commission,
        equity_slippage=equity_slippage,
        max_leverage=1.0,
        same_bar_execution=True,
        price_used_in_order_execution="close",
        currency_reporting=reporting,
        # start_auction_delta=start_auction_delta,
        # end_auction_delta=end_auction_delta
    )

    if result.errors:
        logger.error(result.errors)
    columns = ["portfolio_value", "pnl", "returns", "algorithm_period_return", "benchmark_period_return"]
    for code, performance in result.perf_by_currency.items():
        print(f"\n{code} (returns are fractions; pnl is daily):")
        print(performance[columns].tail().to_string(float_format=lambda value: f"{value:,.6f}"))
    return result

    print(result.perf.head(n=10).to_markdown())


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())
