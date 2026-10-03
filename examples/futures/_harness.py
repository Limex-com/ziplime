"""Shared runner for the Yahoo Finance futures examples.

Each strategy declares the roots it needs and the whole **chain** of every one is loaded, so a
strategy can read the curve and order any contract on it. A strategy that only rolls the front
month pays for that in load time and nothing else.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

from futures_config import (  # noqa: E402
    ASSET_DB_PATH, END, INITIAL_MARGIN_RATE, MAINTENANCE_MARGIN_RATE, ROLL_END, ROLL_START, START,
    STARTING_CASH, TRADING_CALENDAR,
)

from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.finance.commission import PerContract  # noqa: E402
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL  # noqa: E402
from ziplime.finance.margin import FixedRateFuturesMarginModel, NoFuturesMarginModel  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_bundle_service  # noqa: E402

TZ = ZoneInfo("America/New_York")
STRATEGY_DIR = Path(__file__).parent / "strategies"
BUNDLE_NAME = "yahoo_futures_daily"


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running the simulation."""
    spec = importlib.util.spec_from_file_location(f"_fut_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info.setdefault("roots", ["ES"])
    # "roll" strategies run on the recent window, where the front of the stored chain is the
    # market's real front month; everything else gets the long curve window. See futures_config.
    roll_window = info.get("window") == "roll"
    info.setdefault("start", ROLL_START if roll_window else START)
    info.setdefault("end", ROLL_END if roll_window else END)
    info.setdefault("margin", None)
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    return [load_strategy_info(p) for p in sorted(STRATEGY_DIR.glob("f[0-9][0-9]_*.py"))]


async def load_listings(asset_service, roots: list[str]):
    """Every contract listing on the chains of ``roots``, ordered by expiration."""
    listings = []
    for root in roots:
        chain = await asset_service.get_exchange_futures_contracts_by_root(root_symbol=root)
        if not chain:
            raise SystemExit(
                f"No contracts stored for root {root}. "
                f"Run examples/futures/ingest_futures_data_yahoo.py first.")
        listings.extend(chain)
    return listings


async def run_strategy(info: dict):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info["roots"])

    start = datetime.datetime.combine(info["start"], datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(info["end"], datetime.time.max, tzinfo=TZ)

    bundle_service = get_bundle_service()
    bundle, _ = await bundle_service.load_bundle(
        bundle_name=BUNDLE_NAME, bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start, end_date=end, assets=listings, asset_service=asset_service)

    try:
        return await run_simulation(
            start_date=start, end_date=end,
            trading_calendar=TRADING_CALENDAR,
            algorithm_file=info["path"],
            total_cash=STARTING_CASH,
            market_data_source=bundle,
            custom_data_sources=[],
            emission_rate=datetime.timedelta(days=1),
            benchmark_returns=None, benchmark_asset_symbol=None,
            stop_on_error=True,
            asset_service=asset_service,
            future_commission=PerContract(cost=0.85,
                                          exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
                                          min_trade_cost=0.0),
            # A flat slippage keeps the examples readable. VolatilityVolumeShare is the realistic
            # model, and it needs per-contract volume; Yahoo reports almost none on dated
            # contracts, so there is nothing for it to work with here.
            future_slippage=FixedBasisPointsSlippage(),
            equity_slippage=FixedBasisPointsSlippage(),
            futures_margin_model=(
                FixedRateFuturesMarginModel(initial_rate=INITIAL_MARGIN_RATE,
                                            maintenance_rate=MAINTENANCE_MARGIN_RATE)
                if info["margin"] == "fixed" else NoFuturesMarginModel()),
            max_leverage=info.get("max_leverage", 1.0),
            same_bar_execution=True,
            price_used_in_order_execution="close",
            print_algo=False,
        )
    finally:
        await asset_service._asset_repository.engine.dispose()


def summarise(info: dict, result) -> dict:
    perf = result.perf
    return {
        "name": info["name"],
        "description": info["description"],
        "sessions": len(perf),
        "transactions": sum(len(t) for t in perf["transactions"]),
        "final_value": float(perf["portfolio_value"].iloc[-1]),
        "return": float(perf["algorithm_period_return"].iloc[-1]),
        "max_drawdown": float(perf["max_drawdown"].iloc[-1]),
        "errors": list(result.errors or []),
    }
