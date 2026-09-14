"""Everything that reaches into the ziplime engine, in one place.

Imports of the engine are deferred into the functions that need them. Starting
a stdio MCP server means a client is waiting on a handshake, and importing
ziplime pulls in Polars, DeltaLake, exchange calendars and SQLAlchemy — several
seconds. A server that answers `initialize` immediately and pays that cost on
the first real call is the difference between "connected" and "timed out".
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from .errors import InvalidArguments, NotFound, ZiplimeMcpError
from . import workspace

#: What the engine's bar frequencies are called on the wire.
FREQUENCIES = {
    "1m": dt.timedelta(minutes=1),
    "5m": dt.timedelta(minutes=5),
    "15m": dt.timedelta(minutes=15),
    "1h": dt.timedelta(hours=1),
    "1d": dt.timedelta(days=1),
    "1w": dt.timedelta(weeks=1),
}
DEFAULT_CALENDAR = "NYSE"


def frequency(value: str) -> dt.timedelta:
    try:
        return FREQUENCIES[value]
    except KeyError:
        raise InvalidArguments(
            f"Unknown frequency {value!r}",
            hint=f"One of: {', '.join(FREQUENCIES)}.",
        ) from None


def calendar_tz(name: str):
    """The timezone the named exchange calendar keeps its sessions in."""
    from ziplime.utils.calendar_utils import get_calendar

    try:
        return get_calendar(name).tz
    except Exception as exc:  # noqa: BLE001 — the calendar library raises broadly
        raise InvalidArguments(
            f"Unknown trading calendar {name!r}",
            hint="Use an exchange_calendars name such as NYSE, XNYS, XLON or CME.",
            detail=f"{type(exc).__name__}: {exc}",
        ) from None


def parse_date(value: str, field: str, tz=None) -> dt.datetime:
    """A calendar date, made aware in the trading calendar's own timezone.

    **Not UTC.** Bundle rows are stored in the calendar's zone, and Polars
    refuses to compare timestamps across zones rather than converting — so a
    UTC-aware bound fails the window filter with a SchemaError about dtypes,
    from inside the loader, naming neither the argument nor the fix. Measured;
    the engine's own examples localise to the calendar too.
    """
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise InvalidArguments(
            f"{field} must be a date like 2024-01-31, got {value!r}",
        ) from None
    zone = tz or dt.timezone.utc
    return parsed.astimezone(zone) if parsed.tzinfo else parsed.replace(tzinfo=zone)



def _strategy_frame(exc: BaseException, algorithm_path: Path) -> tuple[int, str] | None:
    """The deepest traceback frame inside the strategy file, or None if it is not in there.

    Deepest, not first: the engine calls the strategy, which may call itself, and the
    line a person has to edit is the innermost one that is still theirs.
    """
    import traceback

    wanted = str(algorithm_path)
    for frame in reversed(traceback.extract_tb(exc.__traceback__)):
        if frame.filename == wanted:
            return frame.lineno, (frame.line or "").strip()
    return None


def providers() -> list[dict]:
    """The data connectors this installation actually has."""
    from ziplime.utils.bundle_utils import provider_names

    known = {
        "yahoo": {
            "credentials": "none",
            "what_it_gives": "Historical daily OHLCV for US equities and ETFs. Free.",
        },
        "limex-hub": {
            "credentials": "LIMEX_API_KEY",
            "what_it_gives": "Professional market data plus point-in-time fundamentals.",
        },
        "lime-trader-sdk": {
            "credentials": "LIME_SDK_CREDENTIALS_FILE",
            "what_it_gives": "Real-time and historical data from the broker.",
        },
    }
    return [
        {"name": name, **known.get(name, {"credentials": "unknown", "what_it_gives": ""})}
        for name in provider_names()
    ]


#: Seconds per bar, back to the name the wire uses. Built from FREQUENCIES so the two
#: cannot drift apart.
_SECONDS_TO_FREQUENCY = {int(delta.total_seconds()): name for name, delta in FREQUENCIES.items()}


async def bundles() -> list[dict]:
    """Every ingested bundle on this machine, newest first.

    Only what the registry actually stores. It records a frequency, a calendar and an
    ingest time -- and no date range, so none is reported. Reporting the range as an
    empty string, which is what reading `start_date` off these rows produced, reads as
    "this bundle holds nothing" and sends a model off to re-ingest data that is there.
    """
    from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry

    registry = FileSystemBundleRegistry(base_data_path=workspace.bundle_storage_path())
    rows = await registry.list_bundles()
    out = []
    for row in rows:
        seconds = row.get("frequency_seconds")
        out.append({
            "name": row.get("name"),
            "version": row.get("version"),
            "created": str(row.get("timestamp") or "")[:19],
            "frequency": (
                row.get("frequency_text")
                or _SECONDS_TO_FREQUENCY.get(int(seconds), f"{int(seconds)}s")
                if seconds else None
            ),
            "calendar": row.get("trading_calendar_name"),
            "data_type": row.get("data_type"),
        })
    out.sort(key=lambda item: item.get("created") or "", reverse=True)
    return out


def asset_service():
    from ziplime.core.ingest_data import get_asset_service

    return get_asset_service(db_path=workspace.asset_db_path(), clear_asset_db=False)


async def ingest_instruments(provider: str) -> dict:
    """The instrument catalogue. Has to exist before bars can be resolved."""
    from ziplime.core.ingest_data import ingest_assets
    from ziplime.utils.bundle_utils import get_asset_data_source

    service = asset_service()
    await ingest_assets(asset_service=service, asset_data_source=get_asset_data_source(provider))
    return {"provider": provider, "asset_database": workspace.asset_db_path()}


async def ingest_bars(
    *,
    provider: str,
    bundle: str,
    symbols: list[str],
    start: str,
    end: str,
    freq: str = "1d",
    calendar: str = DEFAULT_CALENDAR,
    merge: bool = False,
) -> dict:
    """Bars for these symbols into a named bundle."""
    from ziplime.assets.domain.asset_type import AssetType
    from ziplime.assets.entities.asset_symbol import AssetSymbol
    from ziplime.core.ingest_data import ingest_market_data
    from ziplime.utils.bundle_utils import get_market_data_source

    service = asset_service()
    # Hand the connector the listings we already know. Yahoo does not return an
    # exchange alongside its bars, and without this it falls back to one
    # metadata lookup per symbol.
    listings = await service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s, mic=None) for s in symbols],
        asset_type=AssetType.EQUITY,
    )
    known = [listing for listing in listings if listing is not None]
    missing = [s for s, listing in zip(symbols, listings) if listing is None]
    if not known:
        raise NotFound(
            "None of those symbols is in the instrument database",
            hint=(
                "Run ingest_instruments once for this provider first — bars are "
                "resolved against the catalogue, so an empty catalogue matches "
                "nothing."
                ),
            detail=f"asked for: {', '.join(symbols)}",
        )

    await ingest_market_data(
        start_date=parse_date(start, "start"),
        end_date=parse_date(end, "end"),
        symbols=symbols,
            trading_calendar=calendar,
        bundle_name=bundle,
        data_bundle_source=get_market_data_source(provider, assets=known),
        data_frequency=frequency(freq),
            asset_service=service,
        asset_type=AssetType.EQUITY,
        merge=merge,
        bundle_storage_path=workspace.bundle_storage_path(),
    )
    result = {
        "bundle": bundle,
        "provider": provider,
        "symbols_ingested": [s for s in symbols if s not in missing],
        "frequency": freq,
        "start": start,
        "end": end,
    }
    if missing:
        result["symbols_not_in_catalogue"] = missing
        result["note"] = (
            "Those symbols are not in the instrument database and were skipped. "
            "They will not resolve in a backtest either."
        )
    return result


async def run_backtest(
    *,
    strategy: str,
    bundle: str,
    start: str,
    end: str,
    capital: float,
    symbols: list[str],
    benchmark: str | None = None,
    freq: str = "1d",
    calendar: str = DEFAULT_CALENDAR,
    config_file: str | None = None,
    commission_per_share: float | None = None,
    slippage_bps: float = 5.0,
    max_leverage: float = 1.0,
    same_bar_execution: bool = False,
    fill_price: str = "close",
    stop_on_error: bool = True,
) -> dict:
    """One simulation, from a strategy in the workspace against a local bundle."""
    import polars as pl
    from ziplime.assets.domain.asset_type import AssetType
    from ziplime.assets.entities.asset_symbol import AssetSymbol
    from ziplime.core.run_simulation import run_simulation
    from ziplime.finance.commission import PerShare
    from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
    from ziplime.utils.bundle_utils import get_bundle_service

    from . import formatting

    algorithm_path = workspace.strategy_path(strategy)
    if not algorithm_path.exists():
        raise NotFound(
            f"No strategy named {strategy!r}",
            hint="write_strategy saves one; list_strategies shows what is here.",
        )

    tz = calendar_tz(calendar)
    start_dt, end_dt = parse_date(start, "start", tz), parse_date(end, "end", tz)
    if end_dt <= start_dt:
        raise InvalidArguments("end must be after start")

    service = asset_service()
    listings = await service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s, mic=None) for s in symbols],
        asset_type=AssetType.EQUITY,
    )
    assets = [listing for listing in listings if listing is not None]
    if not assets:
        raise NotFound(
            "None of those symbols resolved against the instrument database",
            hint="ingest_instruments, then ingest_market_data, then run again.",
            detail=f"asked for: {', '.join(symbols)}",
        )

    bundle_service = get_bundle_service(bundle_storage_path=workspace.bundle_storage_path())
    # Load through the *end of* the last day, not its midnight. A date-only
    # bound parses to 00:00, while the clock runs that session to its close —
    # so asking for an end date that is itself a trading day loaded a slice
    # finishing before the bars the simulation then demanded, and the engine
    # answered "Requested end date … is greater than end date … of the bundle".
    # It looked like a bundle that was too short; the bundle was fine. Only the
    # load bound moves; the simulation window is still what was asked for.
    load_end = (
        end_dt.replace(hour=23, minute=59, second=59)
        if (end_dt.hour, end_dt.minute, end_dt.second) == (0, 0, 0)
        else end_dt
    )
    try:
        market_data, _missing = await bundle_service.load_bundle(
            bundle_name=bundle,
            bundle_version=None,
            frequency=frequency(freq),
                start_date=start_dt,
            end_date=load_end,
            assets=assets,
            aggregations=[
                pl.col("open").first(), pl.col("high").max(), pl.col("low").min(),
                pl.col("close").last(), pl.col("volume").sum(), pl.col("symbol").last(),
            ],
        )
    except Exception as exc:
        raise NotFound(
            f"Could not load bundle {bundle!r} over that window",
            hint=(
                "list_bundles shows what is ingested. A backtest cannot reach "
                "outside the bars on disk."
            ),
            detail=f"{type(exc).__name__}: {exc}",
        ) from exc

    try:
        result = await run_simulation(
            start_date=start_dt,
            end_date=end_dt,
            trading_calendar=calendar,
            emission_rate=frequency(freq),
            total_cash=float(capital),
            market_data_source=market_data,
            custom_data_sources=[],
            algorithm_file=str(algorithm_path),
            config_file=config_file,
            benchmark_asset_symbol=benchmark,
            benchmark_returns=None,
            stop_on_error=stop_on_error,
            asset_service=service,
            equity_commission=(
                PerShare(cost=commission_per_share, min_trade_cost=0.0)
                if commission_per_share is not None else None
            ),
            equity_slippage=FixedBasisPointsSlippage(basis_points=slippage_bps),
            max_leverage=max_leverage,
            # Off, like the command line's default and for the same reason: filling at the
            # close of the bar the decision was taken on is a price the market had not
            # printed yet. It used to be hard-coded on here, so every run through this
            # server was optimistic by a bar of edge and said nothing about it.
            same_bar_execution=same_bar_execution,
            price_used_in_order_execution=fill_price,
            print_algo=False,
        )
    except Exception as exc:
        # One handler, and the order inside it is the point. A strategy can raise
        # ValueError too, so "did this come out of the strategy file" is asked first;
        # only then is a bare ValueError read as the window argument it usually is.
        blame = _strategy_frame(exc, algorithm_path)
        if blame is not None:
            # A strategy that raises is the strategy's problem, and saying so is the
            # difference between a model fixing its code and a model reporting a bug in
            # this server. The catch-all in `server._handled` cannot tell them apart --
            # it called every one of these an internal_error, "a bug in the local MCP
            # server or in the engine beneath it". A traceback frame in the strategy file
            # can.
            line, text = blame
            raise ZiplimeMcpError(
                f"{strategy} raised {type(exc).__name__} at line {line}: {exc}",
                code="strategy_error",
                hint=(
                    "This is the strategy's code, not the engine. Fix it with "
                    "write_strategy and run again. Pass stop_on_error=false to let the "
                    "run continue and collect every error instead of stopping at the first."
                ),
                detail=f"line {line}: {text}" if text else "",
            ) from None
        # The engine raises a bare ValueError when the window reaches past the
        # bars. That is an argument problem, not a crash, and saying so names
        # the one thing the caller can change.
        if isinstance(exc, ValueError) and ("end date" in str(exc) or "start date" in str(exc)):
            raise InvalidArguments(
                "The simulation window reaches outside the data on disk",
                hint=(
                    "Narrow start/end, or ingest a wider range into the bundle. "
                    "list_bundles shows what each one covers."
                ),
                detail=str(exc),
            ) from None
        raise

    backtest_id = workspace.new_backtest_id(strategy)
    meta = {
        "backtest_id": backtest_id,
        "strategy": strategy,
        "parameters": {
            "bundle": bundle, "start": start, "end": end, "capital": capital,
            "symbols": symbols, "benchmark": benchmark, "frequency": freq,
            "calendar": calendar, "commission_per_share": commission_per_share,
            "slippage_bps": slippage_bps, "max_leverage": max_leverage,
            "same_bar_execution": same_bar_execution, "fill_price": fill_price,
        },
        "ran_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "errors": list(result.errors or []),
        "summary": formatting.summarise(result.perf),
    }
    workspace.save_backtest(backtest_id, meta, result.perf)
    return meta
