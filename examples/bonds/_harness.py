"""Shared runner for the bond strategy examples.

Every strategy in ``strategies/`` is a stand-alone algorithm file -- ziplime loads it by path, so
it may not import anything from this package. What it *can* declare is a module-level
``STRATEGY_INFO`` dict, which this harness reads to decide what to load and how to configure the
simulation.

Bars come from :mod:`ziplime.data.data_sources.demo_bonds` rather than from an ingested bundle:
the demo issues are synthetic and no vendor has prices for them. Point ``BOND_TICKERS`` at real
tickers and swap ``build_bundle`` for ``bundle_service.load_bundle`` once a real ingest has run.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

from bond_config import (  # noqa: E402
    ASSET_DB_PATH, EXAMPLE_END, EXAMPLE_START, MIC, STARTING_CASH, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.data_sources.demo_bonds import build_demo_bond_bars  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.finance.commission import PerBondTurnover  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

TZ = ZoneInfo("Europe/Moscow")
STRATEGY_DIR = Path(__file__).parent / "strategies"


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running the simulation."""
    spec = importlib.util.spec_from_file_location(f"_bond_strategy_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info.setdefault("tickers", ["ZLB26"])
    info.setdefault("start", EXAMPLE_START)
    info.setdefault("end", EXAMPLE_END)
    info.setdefault("commission", 0.0003)
    # A strategy that names real tickers needs an ingested bundle, not generated demo bars.
    info.setdefault("real_data", False)
    info["path"] = str(path)
    return info


def list_strategies(include_real_data: bool = False) -> list[dict]:
    """Every strategy in the package, in file order.

    Real-data strategies are left out by default: they need bars ingested from a vendor, and this
    runner is the one that works with no token at all.
    """
    infos = [load_strategy_info(path) for path in sorted(STRATEGY_DIR.glob("b[0-9][0-9]_*.py"))]
    return [i for i in infos if include_real_data or not i["real_data"]]


async def load_listings(asset_service, tickers: list[str]):
    """Resolve tickers to bond listings, failing with a pointer to the seeder if they are absent."""
    listings = []
    for ticker in tickers:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=MIC), asset_type=AssetType.BOND)
        if listing is None:
            raise SystemExit(
                f"No bond {ticker}@{MIC} in the asset database. "
                f"Run `python examples/bonds/seed_demo_bonds.py` first."
            )
        listings.append(listing)
    return listings


def build_bundle(listings, start: datetime.date, end: datetime.date,
                 asset_service=None) -> DataBundle:
    """An in-memory daily bundle of generated clean prices for the demo issues."""
    calendar = get_calendar(TRADING_CALENDAR)
    sessions = calendar.sessions_in_range(start, end)
    # Bars are stamped at the session close, not at midnight: the simulation asks the bundle for
    # data as of the close, and a bundle that ends at midnight is short by a day.
    closes = {session.date(): close.to_pydatetime()
              for session, close in
              calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz).items()}

    frame = build_demo_bond_bars(listings=listings, sessions=list(closes))
    if frame.is_empty():
        raise SystemExit("No bars for the requested bonds in this window.")

    import polars as pl
    frame = frame.with_columns(
        pl.col("date").map_elements(
            closes.__getitem__,
            return_dtype=pl.Datetime(time_unit="us", time_zone=str(calendar.tz)),
        )
    ).sort(["sid", "date"])

    indexes = frame.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}

    bundle_start = frame["date"].min()
    bundle_end = frame["date"].max()
    return DataBundle(
        name="demo_bonds_daily", version="1",
        start_date=bundle_start, end_date=bundle_end,
        trading_calendar=calendar, frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=bundle_end, data=frame, sid_indexes=sid_indexes,
        asset_service=asset_service)


async def run_strategy(info: dict):
    """Run one strategy and return its :class:`TradingAlgorithmExecutionResult`."""
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info["tickers"])

    start = datetime.datetime.combine(info["start"], datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(info["end"], datetime.time.max, tzinfo=TZ)
    bundle = build_bundle(listings, info["start"], info["end"], asset_service=asset_service)

    try:
        return await run_simulation(
            start_date=start,
            end_date=end,
            trading_calendar=TRADING_CALENDAR,
            algorithm_file=info["path"],
            total_cash=STARTING_CASH,
            market_data_source=bundle,
            custom_data_sources=[],
            emission_rate=datetime.timedelta(days=1),
            benchmark_returns=None,
            benchmark_asset_symbol=None,
            stop_on_error=True,
            asset_service=asset_service,
            bond_commission=PerBondTurnover(cost=info["commission"]),
            # A flat slippage keeps the examples readable. Bonds are far less liquid than the
            # equities this default was tuned for, so a real study wants a wider one.
            bond_slippage=FixedBasisPointsSlippage(),
            equity_slippage=FixedBasisPointsSlippage(),
            max_leverage=info.get("max_leverage", 1.0),
            same_bar_execution=True,
            price_used_in_order_execution="close",
            print_algo=False,
        )
    finally:
        await asset_service._asset_repository.engine.dispose()


def summarise(info: dict, result) -> dict:
    """Reduce a run to the numbers the example table shows."""
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
