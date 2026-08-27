"""Backtest a MOEX FORTS futures strategy on the bundle ingested by ingest_data_finam_futures.py."""
import asyncio
import datetime
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import structlog
from finam_futures_config import ASSET_DB_PATH, BUNDLE_NAME, MIC, ROOT_SYMBOLS, TRADING_CALENDAR

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import (
    DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE, DEFAULT_PER_CONTRACT_COST, PerContract,
)
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.logging_utils import configure_logging

logger = structlog.get_logger(__name__)


async def _run_simulation():
    tz = ZoneInfo("Europe/Moscow")
    start_date = datetime.datetime(year=2020, month=3, day=2, tzinfo=tz)
    # End on the session close, not midnight: the simulation asks for a spot value at the last
    # session's close, and the bundle has to extend at least that far.
    end_date = datetime.datetime(year=2026, month=7, day=30, hour=23, minute=59, second=59,
                                 tzinfo=tz)

    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    bundle_service = get_bundle_service()

    # Load the whole chain, not just the contract that happens to be front month today: the
    # simulation walks through the rolls and needs every contract it held along the way.
    exchange_assets = []
    for root_symbol in ROOT_SYMBOLS:
        exchange_assets.extend(await asset_service.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol, mic=MIC))

    market_data_bundle, missing_data = await bundle_service.load_bundle(
        bundle_name=BUNDLE_NAME,
        bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start_date,
        end_date=end_date,
        assets=exchange_assets,
        # Required for continuous futures: the bundle resolves a chain to a contract through it.
        asset_service=asset_service,
    )

    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar=TRADING_CALENDAR,
        algorithm_file=str(Path("algorithms/test_algo/test_algo_finam_futures.py").absolute()),
        config_file=str(Path("algorithms/test_algo/test_algo_finam_futures_config.json").absolute()),
        total_cash=1_000_000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=[],
        emission_rate=datetime.timedelta(days=1),
        benchmark_returns=None,
        benchmark_asset_symbol=None,
        stop_on_error=True,
        asset_service=asset_service,
        future_commission=PerContract(
            cost=DEFAULT_PER_CONTRACT_COST,
            exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE,
        ),
        # VolatilityVolumeShare needs a 20-day volume/volatility window; a flat slippage keeps the
        # example readable. Swap it in for a more realistic fill model.
        future_slippage=FixedBasisPointsSlippage(),
        max_leverage=1.0,
        same_bar_execution=True,
        price_used_in_order_execution="close",
    )

    if result.errors:
        logger.error("Simulation errors", errors=result.errors)
    perf = result.perf
    # A futures position carries exposure but no value, and its P&L arrives as daily variation
    # margin in cash -- so ending_value stays 0 while ending_cash moves every day.
    print(perf[["portfolio_value", "ending_value", "ending_exposure", "ending_cash",
                "pnl", "returns", "longs_count"]].head(8).to_markdown())
    print()
    print(f"transactions: {sum(len(t) for t in perf['transactions'])}, "
          f"final portfolio value: {perf['portfolio_value'].iloc[-1]:,.2f}, "
          f"total return: {perf['algorithm_period_return'].iloc[-1]:.2%}")
    return result


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(_run_simulation())
