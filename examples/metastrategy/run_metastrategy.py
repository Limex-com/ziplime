"""Run a metastrategy: it is an ordinary algorithm file, so run_simulation runs it.

    python examples/metastrategy/run_metastrategy.py meta_cluster.py

Needs a daily bundle with SBER@MISX; set BUNDLE below to its name.
"""
import asyncio
import datetime
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerDollar
from ziplime.utils.bundle_utils import get_bundle_service

BUNDLE = "moex_daily"  # the name you ingested your daily MOEX bars under
HERE = Path(__file__).parent


async def main(algorithm: str) -> None:
    tz = ZoneInfo("Europe/Moscow")
    start = datetime.datetime(2023, 1, 3, tzinfo=tz)
    end = datetime.datetime(2025, 12, 30, 23, 59, tzinfo=tz)
    asset_service = get_asset_service(db_path=str(HERE.parent.parent / "data" / "assets.sqlite"))
    assets = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol="SBER", mic="MISX")], asset_type=AssetType.EQUITY)
    bundle, _ = await get_bundle_service().load_bundle(
        bundle_name=BUNDLE, bundle_version=None, frequency=datetime.timedelta(days=1),
        start_date=start - datetime.timedelta(days=200), end_date=end, assets=assets)

    result = await run_simulation(
        start_date=start, end_date=end, trading_calendar="XMOS",
        emission_rate=datetime.timedelta(days=1), total_cash=1_000_000.0,
        market_data_source=bundle, custom_data_sources=[],
        algorithm_file=str(HERE / algorithm), stop_on_error=True, asset_service=asset_service,
        equity_commission=PerDollar(cost=0.0005), same_bar_execution=False, print_algo=False)

    print("metastrategy:", f"return {result.perf['returns'].add(1).prod() - 1:.2%}",
          f"sharpe {result.perf['sharpe'].iloc[-1]:.2f}")
    for label, strategy in result.strategies.items():   # each strategy's own result
        print(f"  {label}: return {strategy.perf['returns'].add(1).prod() - 1:.2%}, "
              f"sharpe {strategy.perf['sharpe'].iloc[-1]:.2f}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "meta_sharpe_switch.py"))
