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
    ASSET_DB_PATH, CONGRESS_END, CONGRESS_START, CONGRESS_UNIVERSE, EARNINGS_END, EARNINGS_START,
    EARNINGS_UNIVERSE, END, EQUITY_MIC, EQUITY_TICKERS, FUNDAMENTALS_END, FUNDAMENTALS_START,
    FUNDAMENTALS_UNIVERSE, INSIDER10_END, INSIDER10_START, INSIDER10_UNIVERSE, INSIDER_END,
    INSIDER_START, START, STARTING_CASH, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.constants.data_type import DataType  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.domain.data_bundle import DataBundle  # noqa: E402
from ziplime.data.services import frame_cache  # noqa: E402
from ziplime.finance.commission import PerShare  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402

from intraday import (  # noqa: E402
    INTRADAY_UNIVERSE, build_intraday_bundle, recent_window,
)

TZ = ZoneInfo("America/New_York")

#: Sessions a run needs before an annual rate means anything. A quarter of a year.
MIN_SESSIONS_TO_ANNUALISE = 60
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
    elif window == "fundamentals":
        info.setdefault("equities", FUNDAMENTALS_UNIVERSE)
        info.setdefault("start", FUNDAMENTALS_START)
        info.setdefault("end", FUNDAMENTALS_END)
    elif window == "earnings":
        info.setdefault("equities", EARNINGS_UNIVERSE)
        info.setdefault("start", EARNINGS_START)
        info.setdefault("end", EARNINGS_END)
    elif window == "insider10":
        info.setdefault("equities", INSIDER10_UNIVERSE)
        info.setdefault("start", INSIDER10_START)
        info.setdefault("end", INSIDER10_END)
    elif window in ("intraday-1m", "intraday-5m"):
        # The only windows here that are not fixed dates. Yahoo deletes intraday history as it
        # ages -- seven days of one-minute bars, sixty of five-minute -- so the window has to
        # follow the calendar, and these examples demonstrate a mechanism rather than reproduce
        # a number. See `intraday.recent_window`.
        rate = (datetime.timedelta(minutes=1) if window == "intraday-1m"
                else datetime.timedelta(minutes=5))
        start, end = recent_window(rate, sessions=3 if window == "intraday-1m" else 20)
        info.setdefault("equities", INTRADAY_UNIVERSE)
        info.setdefault("emission_rate", rate)
        info.setdefault("start", start)
        info.setdefault("end", end)
    else:
        info.setdefault("equities", [(t, EQUITY_MIC) for t in EQUITY_TICKERS])
        info.setdefault("start", START)
        info.setdefault("end", END)
    info["path"] = str(path)
    return info


def list_strategies() -> list[dict]:
    return [load_strategy_info(p) for p in sorted(STRATEGY_DIR.glob("[hifme][0-9][0-9]_*.py"))]


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


#: One bundle per (universe, window), for the life of the process. Eleven strategies share a
#: universe of 125 names over ten years, and fetching those bars eleven times from Yahoo would
#: dominate the run.
_BUNDLE_CACHE: dict[tuple, "DataBundle"] = {}

#: How long a cached price frame stays usable. A day, because price history is **not** immutable:
#: a split or a dividend restates every bar before it once the source adjusts, so a frame cached
#: last month is wrong after last week's split. A day is short enough that a restatement is caught
#: on the next session and long enough that an afternoon of editing a strategy pays the download
#: once. Delete the cache, or set ZIPLIME_CACHE_DIR elsewhere, to force a refetch.
BARS_MAX_AGE = datetime.timedelta(days=1)


async def build_bundle(listings, start: datetime.date, end: datetime.date, asset_service):
    """Daily Yahoo bars for the example equities, stamped on the calendar's session closes."""
    sids = tuple(sorted(listing.sid for listing in listings))
    key = (sids, start, end)
    cached = _BUNDLE_CACHE.get(key)
    if cached is not None:
        cached.asset_service = asset_service
        return cached

    # Across processes: the same bars, read off the disk instead of off the network.
    disk_key = frame_cache.cache_key("yahoo-daily-bars", sids, start, end)
    data = frame_cache.load(disk_key, max_age=BARS_MAX_AGE)
    if data is not None:
        return _assemble_bundle(data, asset_service, key)
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

    # An **unadjusted** close alongside the adjusted bars.
    #
    # Every price here is back-adjusted for splits, which is right for returns and wrong for
    # anything multiplied by a share count. A filing reports shares as they existed then; a split
    # afterwards divides the historical price but not that number, so `price x shares` understates
    # the market value by the split factor. Deckers is the example that caught it: a 2013 market
    # value came out seven times too small and its earnings yield at 57%, a P/E of 1.8.
    #
    # Ratios built only from filings -- gross profitability, accruals, return on equity -- are
    # unaffected. Only the ones that meet a price need this column.
    raw = await source.get_data(
        symbols=[listing.symbol for listing in listings],
        frequency=datetime.timedelta(days=1),
        date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz),
        auto_adjust=False)
    if not raw.is_empty():
        raw = raw.with_columns(
            pl.col("symbol").replace_strict(sid_by_symbol, return_dtype=pl.Int64).alias("sid"),
            pl.col("date").dt.convert_time_zone(str(calendar.tz)).dt.date().alias("_d"),
        ).filter(pl.col("_d").is_in(list(closes)))
        raw = raw.with_columns(
            pl.col("_d").map_elements(closes.__getitem__,
                                      return_dtype=pl.Datetime(time_unit="us",
                                                               time_zone=str(calendar.tz)))
            .alias("date")).drop("_d")
        frame = frame.join(raw.select(["date", "sid", pl.col("close").alias("unadjusted_close")]),
                           on=["date", "sid"], how="left")
    else:
        frame = frame.with_columns(pl.col("close").alias("unadjusted_close"))

    columns = ["date", "sid", "symbol", "mic", "open", "high", "low", "close", "price", "volume",
               "unadjusted_close"]
    data = frame.select(columns).sort(["sid", "date"])
    frame_cache.store(disk_key, data)
    return _assemble_bundle(data, asset_service, key)


def _assemble_bundle(data, asset_service, key) -> DataBundle:
    """Wrap a bar frame as a bundle and remember it for the rest of the process."""
    indexes = data.with_row_index().group_by("sid", maintain_order=True).agg([
        pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
    sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}

    bundle = DataBundle(
        name="hf_equities_daily", version="1",
        start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=get_calendar(TRADING_CALENDAR), frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1), data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(), data=data, sid_indexes=sid_indexes,
        asset_service=asset_service)
    _BUNDLE_CACHE[key] = bundle
    return bundle


async def run_strategy(info: dict):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    listings = await load_listings(asset_service, info["equities"])
    emission_rate = info.get("emission_rate", datetime.timedelta(days=1))
    if emission_rate < datetime.timedelta(days=1):
        bundle = await build_intraday_bundle(listings, info["start"], info["end"],
                                             emission_rate, asset_service, TRADING_CALENDAR)
    else:
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
            emission_rate=emission_rate,
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
    """Summarise a run, including the risk figures a return on its own cannot be read without.

    A concentrated book beats a diversified one on return simply by being concentrated. Sharpe and
    the return-to-drawdown ratio are what say whether it did so by taking more risk or by taking
    better ones, and every strategy in this directory holds fewer names than its control.
    """
    perf = result.perf
    returns = perf["returns"] if "returns" in perf.columns else None
    total_return = float(perf["algorithm_period_return"].iloc[-1])
    drawdown = float(perf["max_drawdown"].iloc[-1])
    years = len(perf) / 252.0
    # Annualising a run of a few sessions produces a number that is arithmetically correct and
    # says nothing: +1.26% over three days is "+186% a year", and a Sharpe of 6. The intraday
    # examples exist to demonstrate timing over a window Yahoo will still serve, so they report
    # what happened and leave the annual figures blank rather than printing fiction.
    annualisable = len(perf) >= MIN_SESSIONS_TO_ANNUALISE

    sharpe = float("nan")
    volatility = float("nan")
    if returns is not None and len(returns) > 1 and annualisable:
        daily = returns.astype(float)
        volatility = float(daily.std() * (252 ** 0.5))
        if volatility > 0:
            sharpe = float(daily.mean() * 252 / volatility)
    cagr = (((1.0 + total_return) ** (1.0 / years) - 1.0)
            if annualisable and years > 0 and total_return > -1 else float("nan"))

    return {
        "name": info["name"],
        "description": info["description"],
        "sessions": len(perf),
        "transactions": sum(len(t) for t in perf["transactions"]),
        "final_value": float(perf["portfolio_value"].iloc[-1]),
        "return": total_return,
        "cagr": cagr,
        "volatility": volatility,
        "sharpe": sharpe,
        "max_drawdown": drawdown,
        # Return per unit of worst peak-to-trough loss. Crude, but it is the number that stops a
        # concentrated book from looking better than a diversified one purely by being levered
        # to the same market.
        "return_to_drawdown": (total_return / abs(drawdown)) if drawdown else float("nan"),
        "errors": list(result.errors or []),
    }
