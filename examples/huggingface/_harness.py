"""Shared runner for the Hugging Face point-in-time dataset examples.

Only the *prices* are prepared here, straight from Yahoo Finance into an in-memory bundle. The
datasets themselves are never ingested: each strategy mounts what it needs from the Hub on its
first read, which is the mechanism these examples exist to demonstrate.
"""
import datetime
import importlib.util
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from exchange_calendars import get_calendar

sys.path.insert(0, str(Path(__file__).parent))

from hf_config import (  # noqa: E402
    ASSET_DB_PATH, CONGRESS_END, CONGRESS_START, CONGRESS_UNIVERSE, END, EQUITY_MIC,
    EQUITY_TICKERS, INSIDER_END, INSIDER_START, START, STARTING_CASH, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.finance.commission import PerShare  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402

TZ = ZoneInfo("America/New_York")
STRATEGY_DIR = Path(__file__).parent / "strategies"


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running the simulation."""
    spec = importlib.util.spec_from_file_location(f"_hf_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    # Each window comes with the universe that suits it. The datasets cover different decades --
    # insider filings stop in March 2016, congressional disclosures run to 2026 -- so mounting one
    # over the other's window would fetch partitions holding nothing.
    window = info.get("window")
    if window == "insider":
        info.setdefault("equities", [(t, EQUITY_MIC) for t in EQUITY_TICKERS])
        info.setdefault("start", INSIDER_START)
        info.setdefault("end", INSIDER_END)
    elif window == "congress":
        info.setdefault("equities", CONGRESS_UNIVERSE)
        info.setdefault("start", CONGRESS_START)
        info.setdefault("end", CONGRESS_END)
    else:
        info.setdefault("equities", [(t, EQUITY_MIC) for t in EQUITY_TICKERS])
        info.setdefault("start", START)
        info.setdefault("end", END)
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    return [load_strategy_info(p) for p in sorted(STRATEGY_DIR.glob("h[0-9][0-9]_*.py"))]


async def load_listings(asset_service, universe: list[tuple[str, str]]):
    """Resolve ``(ticker, MIC)`` pairs to listings. The MIC is required, not decorative."""
    listings = []
    for ticker, mic in universe:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=AssetType.EQUITY)
        if listing is None:
            raise SystemExit(
                f"{ticker} is not in the asset database on {mic}. "
                f"Run examples/ingest_assets_data_yahoo_finance.py first.")
        listings.append(listing)
    return listings


async def build_bundle(listings, start: datetime.date, end: datetime.date, asset_service):
    """Daily Yahoo bars for the example equities, stamped on the calendar's session closes."""
    calendar = get_calendar(TRADING_CALENDAR)
    sessions = calendar.sessions_in_range(start, end)
    closes = {s.date(): c.to_pydatetime() for s, c in
              calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz).items()}

    source = get_market_data_source("yahoo", assets=listings)
    frame = await source.get_data(
        symbols=[listing.symbol for listing in listings],
        frequency=datetime.timedelta(days=1),
        date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
    if frame.is_empty():
        raise SystemExit("Yahoo returned no bars for the requested equities.")

    sid_by_symbol = {listing.symbol: listing.sid for listing in listings}
    frame = frame.with_columns(
        pl.col("symbol").replace_strict(sid_by_symbol, return_dtype=pl.Int64).alias("sid"),
        pl.col("date").dt.convert_time_zone(str(calendar.tz)).dt.date().alias("_d"),
    ).filter(pl.col("_d").is_in(list(closes)))
    frame = frame.with_columns(
        pl.col("_d").map_elements(closes.__getitem__,
                                  return_dtype=pl.Datetime(time_unit="us",
                                                           time_zone=str(calendar.tz)))
        .alias("date")).drop("_d")

    columns = ["date", "sid", "symbol", "mic", "open", "high", "low", "close", "price", "volume"]
    data = frame.select(columns).sort(["sid", "date"])
    indexes = data.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}

    return DataBundle(
        name="hf_equities_daily", version="1",
        start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(), data=data, sid_indexes=sid_indexes,
        asset_service=asset_service)


async def run_strategy(info: dict):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info["equities"])
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
            equity_commission=PerShare(cost=0.0005, min_trade_cost=0.0),
            equity_slippage=FixedBasisPointsSlippage(),
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
