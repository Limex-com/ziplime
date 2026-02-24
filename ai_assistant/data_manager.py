"""
Yahoo Finance data ingestion manager.

Tracks which symbols and date ranges have been ingested locally so we only
download data when something new is requested.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import pathlib
from typing import Callable

DEFAULT_STATE_PATH = pathlib.Path.home() / ".ziplime" / "ai_assistant_state.json"
DEFAULT_ASSETS_DB  = pathlib.Path(__file__).parent.parent / "data" / "assets.sqlite"
BUNDLE_NAME        = "ai_assistant_yahoo_daily"

# Number of extra calendar days to ingest *before* the requested start date.
# This guarantees bundle_start_date < simulation start_date (avoiding the
# "Start date is before bundle start date" ValueError) and also provides
# warmup bars for indicators like moving averages.
WARMUP_BUFFER_DAYS = 90


class DataManager:
    """
    Manages Yahoo Finance data ingestion for the AI assistant.

    State is persisted in ~/.ziplime/ai_assistant_state.json and tracks:
    - Which symbols have been ingested
    - The ingested date range (single contiguous range)

    If the user requests symbols or a date range not yet cached, the manager
    re-ingests everything (all known symbols + new ones, extended date range).
    """

    def __init__(
        self,
        state_path: pathlib.Path = DEFAULT_STATE_PATH,
        assets_db_path: pathlib.Path = DEFAULT_ASSETS_DB,
        on_progress: Callable[[str], None] | None = None,
    ):
        self.state_path = state_path
        self.assets_db_path = assets_db_path
        self.on_progress = on_progress or (lambda msg: None)
        self._state: dict = self._load_state()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def ensure_data(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """
        Ensure that all requested symbols are available for the given date range.
        Downloads missing data from Yahoo Finance automatically.

        The actual ingestion starts WARMUP_BUFFER_DAYS before start_date so that
        bundle_start_date is always strictly earlier than the simulation start_date,
        preventing the "Start date is before bundle start date" error and providing
        warmup bars for indicator-based strategies.
        """
        # Normalize to UTC midnight, then apply warmup buffer to the start.
        # Add 1 extra day to req_end so the bundle's midnight end timestamp is
        # always strictly later than the simulation's 16:00 ET close on end_date,
        # preventing "Requested end date > bundle end date" errors.
        req_start = _to_utc_midnight(start_date) - datetime.timedelta(days=WARMUP_BUFFER_DAYS)
        req_end   = _to_utc_midnight(end_date) + datetime.timedelta(days=1)

        ingested_symbols  = set(self._state.get("symbols", []))
        ingested_start_s  = self._state.get("start_date")
        ingested_end_s    = self._state.get("end_date")

        ingested_start = _parse_date(ingested_start_s) if ingested_start_s else None
        ingested_end   = _parse_date(ingested_end_s)   if ingested_end_s   else None

        missing_symbols  = set(symbols) - ingested_symbols
        date_range_ok    = (
            ingested_start is not None
            and ingested_end is not None
            and ingested_start <= req_start
            and ingested_end >= req_end
        )

        if not missing_symbols and date_range_ok:
            return  # All data already available

        # Compute the union of required + already ingested
        all_symbols   = sorted(ingested_symbols | set(symbols))
        union_start   = min(req_start, ingested_start) if ingested_start else req_start
        union_end     = max(req_end, ingested_end)     if ingested_end   else req_end

        self.on_progress(
            f"Downloading data for {', '.join(all_symbols)} "
            f"({union_start.date()} → {union_end.date()}) from Yahoo Finance…"
        )

        await self._ingest(all_symbols, union_start, union_end)

        # Persist updated state
        self._state = {
            "symbols":    all_symbols,
            "start_date": union_start.strftime("%Y-%m-%d"),
            "end_date":   union_end.strftime("%Y-%m-%d"),
        }
        self._save_state()
        self.on_progress("Data download complete.")

    def get_bundle_name(self) -> str:
        return BUNDLE_NAME

    def get_assets_db_path(self) -> pathlib.Path:
        return self.assets_db_path

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    async def _ingest(
        self,
        symbols: list[str],
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> None:
        """Run Yahoo Finance ingestion (runs in executor to stay async-friendly)."""
        # Import here to avoid circular imports at module load time
        from ziplime.core.ingest_data import get_asset_service, ingest_market_data
        from ziplime.data.data_sources.yahoo_finance_data_source import YahooFinanceDataSource

        asset_service = get_asset_service(
            clear_asset_db=False,
            db_path=str(self.assets_db_path),
        )
        data_source = YahooFinanceDataSource(maximum_threads=4)

        await ingest_market_data(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            trading_calendar="NYSE",
            bundle_name=BUNDLE_NAME,
            data_bundle_source=data_source,
            data_frequency=datetime.timedelta(days=1),
            asset_service=asset_service,
        )

    def _load_state(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except Exception:
                return {}
        return {}

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self._state, indent=2))


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _to_utc_midnight(dt: datetime.datetime) -> datetime.datetime:
    """Convert any datetime to UTC-aware midnight."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _parse_date(s: str) -> datetime.datetime:
    return datetime.datetime.strptime(s, "%Y-%m-%d").replace(
        tzinfo=datetime.timezone.utc
    )
