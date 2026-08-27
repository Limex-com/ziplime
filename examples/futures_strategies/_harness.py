"""Shared runner for the futures strategy examples.

Every strategy in ``strategies/`` is a stand-alone algorithm file -- ziplime loads it by path, so
it may not import anything from this package. What it *can* declare is a module-level
``STRATEGY_INFO`` dict, which this harness reads to decide what data to load and how to configure
the simulation.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.finance.commission import PerContract
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
from ziplime.finance.margin import (
    FixedRateFuturesMarginModel, NoFuturesMarginModel, PerRootFuturesMarginModel,
)
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.utils.bundle_utils import get_bundle_service

TZ = ZoneInfo("Europe/Moscow")
CALENDAR = "XMOS"
MIC = "RTSX"
BUNDLE_NAME = "finam_futures_daily"
#: The cross-venue natural gas example has its own bundle: those contracts are quoted in dollars
#: and would otherwise mix currencies with the rouble-quoted examples.
NATGAS_BUNDLE_NAME = "finam_natgas_arbitrage"
ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())
STRATEGY_DIR = Path(__file__).parent / "strategies"

#: Default window. Every root except GL has bars from 2021-01.
DEFAULT_START = datetime.date(2021, 6, 1)
DEFAULT_END = datetime.date(2026, 6, 30)

STARTING_CASH = 1_000_000.0

MARGIN_MODELS = {
    None: lambda: NoFuturesMarginModel(),
    "none": lambda: NoFuturesMarginModel(),
    "fixed": lambda: FixedRateFuturesMarginModel(initial_rate=0.15, maintenance_rate=0.12),
    # Amounts are per contract in the currency each exchange collects: roubles for MOEX --
    # including its dollar-quoted natural gas contract -- and dollars for NYMEX.
    "per_root": lambda: PerRootFuturesMarginModel(
        initial_by_root={"Si": 13_700.0, "SR": 4_800.0, "GZ": 1_500.0,
                         "MX": 39_000.0, "GL": 2_300.0, "NG": 6_556.0,
                         "NG.XNYM": 3_500.0},
        default_per_contract=5_000.0, maintenance_ratio=0.8),
}


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running the simulation."""
    spec = importlib.util.spec_from_file_location(f"_strategy_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info.setdefault("roots", ["Si"])
    info.setdefault("start", DEFAULT_START)
    info.setdefault("end", DEFAULT_END)
    info.setdefault("margin", None)
    info.setdefault("same_bar_execution", True)
    info.setdefault("roll_finder_settings", None)
    info.setdefault("bundle", BUNDLE_NAME)
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    """Every strategy in the package, in file order."""
    return [load_strategy_info(path) for path in sorted(STRATEGY_DIR.glob("s[0-9][0-9]_*.py"))]


async def run_strategy(info: dict):
    """Run one strategy and return its :class:`TradingAlgorithmExecutionResult`."""
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    bundle_service = get_bundle_service()

    start = datetime.datetime.combine(info["start"], datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(info["end"], datetime.time.max, tzinfo=TZ)

    # Load every contract of every root the strategy touches: the simulation walks through the
    # rolls and needs the contracts it held along the way, not just today's front month.
    # No MIC filter -- roots can live on different venues, and each listing carries its own.
    contracts = []
    for root_symbol in info["roots"]:
        contracts.extend(await asset_service.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol))
    if not contracts:
        raise SystemExit(f"No contracts for {info['roots']}. Run the ingest examples first.")

    market_data_bundle, _ = await bundle_service.load_bundle(
        bundle_name=info["bundle"], bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start, end_date=end, assets=contracts,
        # Required for continuous futures: the bundle resolves a chain to a contract through it.
        asset_service=asset_service,
        roll_finder_settings=info["roll_finder_settings"])

    return await run_simulation(
        start_date=start,
        end_date=end,
        trading_calendar=CALENDAR,
        algorithm_file=info["path"],
        total_cash=STARTING_CASH,
        market_data_source=market_data_bundle,
        custom_data_sources=[],
        emission_rate=datetime.timedelta(days=1),
        benchmark_returns=None,
        benchmark_asset_symbol=None,
        stop_on_error=True,
        asset_service=asset_service,
        future_commission=PerContract(cost=0.85, exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
                                      min_trade_cost=0.0),
        # A flat slippage keeps the examples readable; VolatilityVolumeShare is the realistic one.
        future_slippage=FixedBasisPointsSlippage(),
        futures_margin_model=MARGIN_MODELS[info["margin"]](),
        max_leverage=info.get("max_leverage", 1.0),
        same_bar_execution=info["same_bar_execution"],
        price_used_in_order_execution="close",
        # The examples runner prints a table, not twenty algorithm listings.
        print_algo=False,
    )


def summarise(info: dict, result) -> dict:
    """Reduce a run to the numbers the example table shows."""
    perf = result.perf
    transactions = sum(len(t) for t in perf["transactions"])
    final = float(perf["portfolio_value"].iloc[-1])
    return {
        "name": info["name"],
        "description": info["description"],
        "sessions": len(perf),
        "transactions": transactions,
        "final_value": final,
        "return": float(perf["algorithm_period_return"].iloc[-1]),
        "max_drawdown": float(perf["max_drawdown"].iloc[-1]),
        "errors": list(result.errors or []),
    }
