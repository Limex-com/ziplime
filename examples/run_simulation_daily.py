import asyncio
import datetime
import logging
import pathlib
from zoneinfo import ZoneInfo

import polars as pl
import structlog

from ziplime.assets.domain.asset_type import AssetType
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging
from ziplime.assets.entities.asset_symbol import AssetSymbol

from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE, \
    PerDollar
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

logger = structlog.get_logger(__name__)


async def _run_simulation():
    tz = ZoneInfo("America/New_York")
    start_date = datetime.datetime(year=2025, month=6, day=3, tzinfo=tz)
    end_date = datetime.datetime(year=2025, month=6, day=9, hour=23, minute=59 , second=59, tzinfo=tz)
    start_auction_delta = datetime.timedelta(minutes=15)
    end_auction_delta = datetime.timedelta(minutes=15)
    # Backtest completed in 3 seconds
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
    symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL", "ETSY", "NVDA", "MSFT"]

    exchange_assets = await asset_service.get_exchange_assets_by_symbols(symbols=[AssetSymbol(
        symbol=symbol, mic=None
    ) for symbol in symbols], asset_type=AssetType.EQUITY)

    market_data_bundle, missing_data = await bundle_service.load_bundle(bundle_name="limex_us_minute_data",
                                                          bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=start_date,
                                                          end_date=end_date,
                                                          assets=exchange_assets,
                                                          start_auction_delta=start_auction_delta,
                                                          end_auction_delta=end_auction_delta,
                                                          aggregations=aggregations,
                                                          )



    custom_data_sources = []
    # custom_data_sources.append(
    #     await bundle_service.load_bundle(bundle_name="limex_us_fundamental_data", bundle_version=None))

    # equity_commission = PerShare(
    #     cost=DEFAULT_PER_SHARE_COST,
    #     min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,
    #
    # )
    equity_commission = PerDollar(
        cost=0.001,
    )
    equity_slippage = FixedBasisPointsSlippage()

    # run daily simulation
    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=custom_data_sources,
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
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
        # start_auction_delta=start_auction_delta,
        # end_auction_delta=end_auction_delta
    )

    if result.errors:
        logger.error(result.errors)
    print(result.perf.head(n=10).to_markdown())

    cash_flow = {"date": [], "cash_change": [], "cash_left": []}

    # Get cash from algo
    # start_cash = sum(exchange.get_start_cash_balance() for exchange in result.trading_algorithm.exchanges.values())
    # logger.info(f"Starting_cash: {start_cash}")

    #
    #         start_cash -=  transaction.total_price()
    #         # print("Start cash: ", start_cash,)
    #         cash_flow["date"].append(transaction.dt)
    #         cash_flow["cash_change"].append(-transaction.total_price())
    #         cash_flow["cash_left"].append(start_cash)

    # cash_flow_df = pl.DataFrame(data=cash_flow)
    # print(cash_flow)

if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(_run_simulation())
