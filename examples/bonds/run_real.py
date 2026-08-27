"""Run the real-data bond example against the ingested MOEX bundle.

Requires both ingest steps to have run:

    export FINAM_API_SECRET=tapi_sk_...
    python examples/ingest_assets_data_finam_bonds.py SU26238RMFS4
    python examples/ingest_data_finam_bonds.py SU26238RMFS4
    python examples/bonds/run_real.py
"""
import asyncio
import datetime
import logging
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

from bond_config import (  # noqa: E402
    ASSET_DB_PATH, BOND_BUNDLE_NAME, INGEST_END, INGEST_START, MIC, STARTING_CASH,
    TRADING_CALENDAR,
)
from _harness import load_listings, summarise  # noqa: E402

from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.finance.commission import PerBondTurnover  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_bundle_service  # noqa: E402
from ziplime.utils.logging_utils import configure_logging  # noqa: E402

TZ = ZoneInfo("Europe/Moscow")
STRATEGY = Path(__file__).parent / "strategies" / "b07_real_ofz_buy_and_hold.py"
TICKERS = ["SU26238RMFS4"]


async def main():
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, TICKERS)

    start = datetime.datetime.combine(INGEST_START, datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(INGEST_END, datetime.time.max, tzinfo=TZ)

    bundle_service = get_bundle_service()
    bundle, _ = await bundle_service.load_bundle(
        bundle_name=BOND_BUNDLE_NAME, bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start, end_date=end, assets=listings,
        asset_service=asset_service)

    result = await run_simulation(
        start_date=start, end_date=end,
        trading_calendar=TRADING_CALENDAR,
        algorithm_file=str(STRATEGY),
        total_cash=STARTING_CASH,
        market_data_source=bundle,
        custom_data_sources=[],
        emission_rate=datetime.timedelta(days=1),
        benchmark_returns=None, benchmark_asset_symbol=None,
        stop_on_error=True,
        asset_service=asset_service,
        bond_commission=PerBondTurnover(cost=0.0003),
        bond_slippage=FixedBasisPointsSlippage(),
        equity_slippage=FixedBasisPointsSlippage(),
        max_leverage=1.0, same_bar_execution=True,
        price_used_in_order_execution="close",
        print_algo=False,
    )

    row = summarise({"name": STRATEGY.stem, "description": "real OFZ 26238"}, result)
    print(f"\nsessions {row['sessions']}  trades {row['transactions']}  "
          f"final {row['final_value']:,.2f}  return {row['return']:+.2%}  "
          f"max dd {row['max_drawdown']:+.2%}")
    if row["errors"]:
        print("errors:", row["errors"][:2])
    await asset_service._asset_repository.engine.dispose()
    return 1 if row["errors"] else 0


if __name__ == "__main__":
    configure_logging(level=logging.CRITICAL, file_name="mylog.log")
    sys.exit(asyncio.run(main()))
