"""Shared runner for the cross-asset examples.

Nothing here is specific to a class: the point of these examples is that one simulation, one
ledger and one cash balance handle equities, bonds and futures at once, so the harness configures
a commission and slippage model per class and otherwise stays out of the way.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

from cross_asset_config import (  # noqa: E402
    ASSET_DB_PATH, BOND_MIC, BUNDLE_NAME, END, EQUITY_MIC, FUTURES_MIC, START, STARTING_CASH,
    TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.finance.commission import PerBondTurnover, PerContract, PerShare  # noqa: E402
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL  # noqa: E402
from ziplime.finance.margin import FixedRateFuturesMarginModel  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_bundle_service  # noqa: E402

TZ = ZoneInfo("Europe/Moscow")
STRATEGY_DIR = Path(__file__).parent / "strategies"

#: Which MIC each class trades on, for resolving a strategy's declared instruments.
MIC_BY_TYPE = {
    AssetType.EQUITY: EQUITY_MIC,
    AssetType.BOND: BOND_MIC,
    AssetType.FUTURES_CONTRACT: FUTURES_MIC,
}


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running the simulation."""
    spec = importlib.util.spec_from_file_location(f"_xa_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info.setdefault("equities", [])
    info.setdefault("bonds", [])
    info.setdefault("futures", [])
    info.setdefault("start", START)
    info.setdefault("end", END)
    info.setdefault("margin", "fixed")
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    return [load_strategy_info(p) for p in sorted(STRATEGY_DIR.glob("x[0-9][0-9]_*.py"))]


async def load_listings(asset_service, info: dict):
    """Resolve every instrument a strategy declares, whatever class it belongs to."""
    wanted = ([(t, AssetType.EQUITY) for t in info["equities"]]
              + [(t, AssetType.BOND) for t in info["bonds"]]
              + [(t, AssetType.FUTURES_CONTRACT) for t in info["futures"]])
    listings = []
    for ticker, asset_type in wanted:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=MIC_BY_TYPE[asset_type]),
            asset_type=asset_type)
        if listing is None:
            raise SystemExit(
                f"{ticker} is not in the asset database as {asset_type.value}. "
                f"Run examples/cross_asset/ingest_cross_asset_data.py and the reference ingests.")
        listings.append(listing)
    return listings


async def run_strategy(info: dict):
    """Run one cross-asset strategy and return its execution result."""
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info)

    start = datetime.datetime.combine(info["start"], datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(info["end"], datetime.time.max, tzinfo=TZ)

    bundle_service = get_bundle_service()
    bundle, _ = await bundle_service.load_bundle(
        bundle_name=BUNDLE_NAME, bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start, end_date=end, assets=listings,
        asset_service=asset_service)

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
            # One model per class. They coexist in one exchange, keyed by instrument type.
            equity_commission=PerShare(cost=0.0005, min_trade_cost=0.0),
            bond_commission=PerBondTurnover(cost=0.0003),
            future_commission=PerContract(cost=0.85,
                                          exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
                                          min_trade_cost=0.0),
            equity_slippage=FixedBasisPointsSlippage(),
            bond_slippage=FixedBasisPointsSlippage(),
            future_slippage=FixedBasisPointsSlippage(),
            futures_margin_model=(
                FixedRateFuturesMarginModel(initial_rate=0.15, maintenance_rate=0.12)
                if info["margin"] == "fixed" else None),
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
