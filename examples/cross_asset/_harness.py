"""Shared runner for the cross-asset examples.

The point of these examples is that one simulation, one ledger and one cash balance handle
equities and bonds at once, so the harness configures a commission and slippage model per class
and otherwise stays out of the way.

The bundle is built here rather than ingested, because the two classes come from different places:
equity bars are fetched from Yahoo Finance (no credentials), and the demo bonds are synthetic, so
their bars are generated. A simulation reads from a **single** market data source, so both have to
end up in one frame -- which is the part worth copying if you point these at real data.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent))

import polars as pl  # noqa: E402

from cross_asset_config import (  # noqa: E402
    ASSET_DB_PATH, BOND_MIC, END, EQUITY_MIC, START, STARTING_CASH, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.data_sources.demo_bonds import build_demo_bond_bars  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.finance.commission import PerBondTurnover, PerShare  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

TZ = ZoneInfo("America/New_York")
STRATEGY_DIR = Path(__file__).parent / "strategies"

MIC_BY_TYPE = {AssetType.EQUITY: EQUITY_MIC, AssetType.BOND: BOND_MIC}


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
    info.setdefault("start", START)
    info.setdefault("end", END)
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    return [load_strategy_info(p) for p in sorted(STRATEGY_DIR.glob("x[0-9][0-9]_*.py"))]


async def load_listings(asset_service, info: dict):
    """Resolve every instrument a strategy declares, whatever class it belongs to."""
    wanted = ([(t, AssetType.EQUITY) for t in info["equities"]]
              + [(t, AssetType.BOND) for t in info["bonds"]])
    listings = []
    for ticker, asset_type in wanted:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=MIC_BY_TYPE[asset_type]), asset_type=asset_type)
        if listing is None:
            raise SystemExit(
                f"{ticker} is not in the asset database as {asset_type.value}. "
                f"Run examples/bonds/seed_demo_bonds.py for the demo bonds, and "
                f"examples/ingest_assets_data_yahoo_finance.py for the equities.")
        listings.append(listing)
    return listings


async def build_bundle(listings, start: datetime.date, end: datetime.date, asset_service):
    """One bundle holding both classes: Yahoo bars for the equities, generated bars for the bonds."""
    calendar = get_calendar(TRADING_CALENDAR)
    sessions = calendar.sessions_in_range(start, end)
    closes = {s.date(): c.to_pydatetime() for s, c in
              calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz).items()}

    equities = [ea for ea in listings if type(ea.asset).__name__ == "Equity"]
    bonds = [ea for ea in listings if type(ea.asset).__name__ == "Bond"]

    frames = []
    if equities:
        source = get_market_data_source("yahoo", assets=equities)
        frame = await source.get_data(
            symbols=[ea.symbol for ea in equities],
            frequency=datetime.timedelta(days=1),
            date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
            date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
        if frame.is_empty():
            raise SystemExit("Yahoo returned no bars for the requested equities.")
        sid_by_symbol = {ea.symbol: ea.sid for ea in equities}
        frame = frame.with_columns(
            pl.col("symbol").replace_strict(sid_by_symbol, return_dtype=pl.Int64).alias("sid"),
            pl.col("date").dt.convert_time_zone(str(calendar.tz)).dt.date().alias("_d"),
        ).filter(pl.col("_d").is_in(list(closes)))
        frame = frame.with_columns(
            pl.col("_d").map_elements(closes.__getitem__,
                                      return_dtype=pl.Datetime(time_unit="us",
                                                               time_zone=str(calendar.tz)))
            .alias("date")).drop("_d")
        frames.append(frame)

    if bonds:
        frame = build_demo_bond_bars(listings=bonds, sessions=list(closes))
        frame = frame.with_columns(
            pl.col("date").map_elements(closes.__getitem__,
                                        return_dtype=pl.Datetime(time_unit="us",
                                                                 time_zone=str(calendar.tz))))
        frames.append(frame)

    columns = ["date", "sid", "symbol", "mic", "open", "high", "low", "close", "price", "volume"]
    data = pl.concat([f.select(columns) for f in frames], how="vertical").sort(["sid", "date"])

    indexes = data.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}

    return DataBundle(
        name="cross_asset_daily", version="1",
        start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(), data=data, sid_indexes=sid_indexes,
        asset_service=asset_service)


async def run_strategy(info: dict):
    """Run one cross-asset strategy and return its execution result."""
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info)
    bundle = await build_bundle(listings, info["start"], info["end"], asset_service)

    start = datetime.datetime.combine(info["start"], datetime.time.min, tzinfo=TZ)
    end = datetime.datetime.combine(info["end"], datetime.time.max, tzinfo=TZ)
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
            equity_slippage=FixedBasisPointsSlippage(),
            bond_slippage=FixedBasisPointsSlippage(),
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
