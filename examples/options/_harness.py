"""Shared runner for the 0DTE option examples.

Assembles one bundle holding SPY's real intraday bars and a synthetic 0DTE chain written on them,
then runs a strategy against it.

**What this harness deliberately does not print is a return.** The option prices are generated,
and a Sharpe ratio computed on generated prices is a statement about the generator. The summary
reports what the run *did* -- chains listed, contracts traded, structures opened, how they settled
at expiry -- which is what a development harness is for. :func:`summarise` calls
:func:`~ziplime.data.data_sources.options.synthetic.refuse_performance_claims` so that the
restriction is enforced by the code rather than remembered by the reader.
"""
import datetime
import importlib.util
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

from options_config import (  # noqa: E402
    ASSET_DB_PATH, EMISSION_RATE, FAR_REACH, FAR_STEP, NEAR_REACH, NEAR_STEP, SESSIONS,
    STARTING_CASH, TRADING_CALENDAR, UNDERLYING,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service  # noqa: E402
from ziplime.core.run_simulation import run_simulation  # noqa: E402
from ziplime.data.data_sources.options.ingest import build_option_bundle  # noqa: E402
from ziplime.data.data_sources.options.synthetic import (  # noqa: E402
    ChainSpec, SyntheticOptionChainSource, refuse_performance_claims,
)
from ziplime.data.services import frame_cache  # noqa: E402
from ziplime.data.services.bar_alignment import align_to_clock, clock_minutes  # noqa: E402
from ziplime.finance.commission import PerOptionContract, PerShare  # noqa: E402
from ziplime.finance.slippage.fixed_basis_points_slippage import (  # noqa: E402
    FixedBasisPointsSlippage,
)
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

STRATEGY_DIR = Path(__file__).parent / "strategies"

#: Yahoo's intraday horizon at five minutes. Anything older simply is not served.
INTRADAY_HORIZON = datetime.timedelta(days=59)


def list_strategies() -> list[dict]:
    return [load_strategy_info(path) for path in sorted(STRATEGY_DIR.glob("o[0-9][0-9]_*.py"))]


def load_strategy_info(path: Path) -> dict:
    """Read a strategy's ``STRATEGY_INFO`` without running it."""
    spec = importlib.util.spec_from_file_location(f"_options_info_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    info = dict(getattr(module, "STRATEGY_INFO", {}))
    info.setdefault("name", path.stem)
    info.setdefault("description", (module.__doc__ or "").strip().split("\n")[0])
    info["path"] = str(path)
    return info


def recent_sessions(sessions: int, calendar_name: str = TRADING_CALENDAR) -> list[datetime.date]:
    """The last complete sessions Yahoo still serves at this interval."""
    calendar = get_calendar(calendar_name)
    today = datetime.date.today()
    available = calendar.sessions_in_range(today - INTRADAY_HORIZON + datetime.timedelta(days=1),
                                           today)
    complete = [s.date() for s in available if s.date() < today]
    if len(complete) < sessions:
        raise SystemExit(f"Only {len(complete)} complete sessions available.")
    return complete[-sessions:]


async def load_underlying(asset_service):
    symbol, mic = UNDERLYING
    listing = await asset_service.get_exchange_asset_by_symbol(
        symbol=AssetSymbol(symbol=symbol, mic=mic), asset_type=AssetType.EQUITY)
    if listing is None:
        raise SystemExit(f"{symbol} is not in the asset database on {mic}. "
                         f"Run examples/ingest_assets_data_yahoo_finance.py first.")
    return listing


async def underlying_bars(listing, sessions: list[datetime.date],
                          emission_rate: datetime.timedelta) -> pl.DataFrame:
    """SPY's real intraday bars, stamped on the simulation clock's grid.

    The alignment is the same one every intraday example does and for the same reason: Yahoo
    labels a bar with the time it *starts*, and stamping it there hands the strategy that bar's
    own future. See :mod:`ziplime.data.services.bar_alignment`.
    """
    calendar = get_calendar(TRADING_CALENDAR)
    start, end = sessions[0], sessions[-1]
    key = frame_cache.cache_key("options-underlying", listing.sid, start, end, str(emission_rate))
    cached = frame_cache.load(key, max_age=datetime.timedelta(hours=1))
    if cached is not None:
        return cached

    source = get_market_data_source("yahoo", assets=[listing])
    frame = await source.get_data(
        symbols=[listing.symbol], frequency=emission_rate,
        date_from=datetime.datetime.combine(start, datetime.time.min, tzinfo=calendar.tz),
        date_to=datetime.datetime.combine(end, datetime.time.max, tzinfo=calendar.tz))
    if frame.is_empty():
        raise SystemExit(f"Yahoo returned no {emission_rate} bars for {listing.symbol}.")

    frame = frame.with_columns(
        pl.lit(listing.sid).alias("sid"),
        pl.col("date").dt.convert_time_zone(str(calendar.tz)),
    ).sort("date")
    grid = clock_minutes(calendar, start, end, emission_rate)
    frame = align_to_clock(frame, grid, emission_rate)
    data = frame.select(["date", "sid", "symbol", "mic", "open", "high", "low", "close",
                         "price", "volume"])
    frame_cache.store(key, data)
    return data


async def build_bundle(asset_service, sessions: list[datetime.date],
                       emission_rate: datetime.timedelta):
    """SPY's real bars plus a synthetic 0DTE chain on each session, in one bundle."""
    calendar = get_calendar(TRADING_CALENDAR)
    listing = await load_underlying(asset_service)
    bars = await underlying_bars(listing, sessions, emission_rate)

    closes = {session.date(): close.to_pydatetime()
              for session, close in calendar.schedule.loc[
                  calendar.sessions_in_range(sessions[0], sessions[-1]), "close"
              ].dt.tz_convert(calendar.tz).items()}

    source = SyntheticOptionChainSource(
        underlying_symbol=listing.symbol, mic=listing.mic,
        underlying_bars=bars, session_closes=closes,
        chain=ChainSpec(near_step=NEAR_STEP, near_reach=NEAR_REACH,
                        far_step=FAR_STEP, far_reach=FAR_REACH))

    bundle, option_listings = await build_option_bundle(
        source=source, asset_service=asset_service, underlying=listing,
        underlying_bars=bars, sessions=sessions,
        timestamps=clock_minutes(calendar, sessions[0], sessions[-1], emission_rate),
        trading_calendar=calendar, emission_rate=emission_rate)
    return bundle, listing, option_listings, source


async def run_strategy(info: dict):
    """Run one strategy and return ``(result, context)`` describing what it ran on."""
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    emission_rate = info.get("emission_rate", EMISSION_RATE)
    sessions = recent_sessions(info.get("sessions", SESSIONS))
    bundle, underlying, option_listings, source = await build_bundle(
        asset_service, sessions, emission_rate)

    calendar = get_calendar(TRADING_CALENDAR)
    start = datetime.datetime.combine(sessions[0], datetime.time.min, tzinfo=calendar.tz)
    end = datetime.datetime.combine(sessions[-1], datetime.time.max, tzinfo=calendar.tz)
    try:
        result = await run_simulation(
            start_date=start, end_date=end, trading_calendar=TRADING_CALENDAR,
            algorithm_file=info["path"], total_cash=STARTING_CASH,
            market_data_source=bundle, custom_data_sources=[],
            emission_rate=emission_rate, benchmark_returns=None, benchmark_asset_symbol=None,
            stop_on_error=True, asset_service=asset_service,
            equity_commission=PerShare(cost=0.0005, min_trade_cost=0.0),
            equity_slippage=FixedBasisPointsSlippage(),
            option_commission=PerOptionContract(),
            max_leverage=info.get("max_leverage", 1.0),
            same_bar_execution=True, price_used_in_order_execution="close", print_algo=False)
        return result, {"sessions": sessions, "contracts": len(option_listings),
                        "underlying": underlying, "source": source}
    finally:
        await asset_service._asset_repository.engine.dispose()


def summarise(info: dict, result, context: dict) -> dict:
    """What the run did. Not what it earned -- see the module docstring."""
    try:
        refuse_performance_claims(context["source"])
        performance_is_meaningful = True
    except Exception:
        performance_is_meaningful = False

    perf = result.perf
    transactions = [t for row in perf["transactions"] for t in row]
    option_trades = [t for t in transactions if hasattr(t.asset.asset, "strike")]
    return {
        "name": info["name"],
        "description": info["description"],
        "sessions": len(context["sessions"]),
        "bars": len(perf),
        "contracts_listed": context["contracts"],
        "option_trades": len(option_trades),
        "contracts_traded": sum(abs(t.amount) for t in option_trades),
        "distinct_contracts": len({t.asset.sid for t in option_trades}),
        # Present so that nothing downstream has to guess whether it may quote a return.
        "performance_is_meaningful": performance_is_meaningful,
        "errors": list(result.errors or []),
    }
