import polars as pl
import datetime

import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period
from ziplime.assets.entities.asset_symbol import AssetSymbol

_logger = structlog.get_logger(__name__)
async def _process_data(data: pl.DataFrame,
                        date_start: datetime.datetime,
                        date_end: datetime.datetime,
                        data_frequency_use_window_end: bool,
                        asset_service: AssetService,
                        name: str,
                        symbols: list[str],
                        trading_calendar: ExchangeCalendar,
                        frequency: datetime.timedelta | Period, ):
    """Ingest data for a given bundle.        """
    _logger.info(f"Ingesting custom bundle: name={name}, date_start={date_start}, date_end={date_end}, "
                      f"symbols={symbols}, frequency={frequency}")
    if date_start < trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
        raise ValueError(
            f"Date start must be after first session of trading calendar. "
            f"First session is {trading_calendar.first_session.replace(tzinfo=trading_calendar.tz)} "
            f"and date start is {date_start}")

    if date_end > trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
        raise ValueError(
            f"Date end must be before last session of trading calendar. "
            f"Last session is {trading_calendar.last_session.replace(tzinfo=trading_calendar.tz)} "
            f"and date end is {date_end}")

    if data.is_empty():
        _logger.warning(
            f"No data for symbols={symbols}, frequency={frequency}, date_start={date_start},"
            f"date_end={date_end} found. Skipping ingestion."
        )
        return

    # repair data
    all_bars = [
        s for s in pl.from_pandas(
            trading_calendar.sessions_minutes(start=date_start.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None),
                                              end=date_end.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)).tz_convert(trading_calendar.tz)
        ) if s >= date_start and s <= date_end
    ]

    required_sessions = pl.DataFrame({"date": all_bars}).group_by_dynamic(
        index_column="date", every=frequency
    ).agg()
    if data_frequency_use_window_end:
        if (
                (type(frequency) is datetime.timedelta and frequency >= datetime.timedelta(days=1)) or
                (type(frequency) is str and frequency in ["1d", "1w", "1mo", "1q", "1y"])
        ):
            last_row = required_sessions.tail(1).with_columns(
                pl.col("date").dt.offset_by(frequency) - pl.duration(days=1))
            required_sessions = required_sessions.with_columns(
                pl.col("date") - pl.duration(days=1)
            )[1:]

            required_sessions = pl.concat([required_sessions, last_row])
    required_columns = [
        "date"
    ]
    missing = [c for c in required_columns if c not in data.columns]

    if missing:
        raise ValueError(f"Ingested data is missing required columns: {missing}. Cannot ingest bundle.")
    if "symbol" not in data.columns and "sid" not in data.columns:
        raise ValueError(f"When ingesting custom bundle you must supply either a symbol or a sid column.")

    sid_id = "sid" in data.columns
    symbol_id = "symbol" in data.columns

    asset_identifiers = list(data["sid"].unique()) if sid_id else list(data["symbol"].unique())

    if sid_id:
        data = await _backfill_symbol_data(data=data, asset_service=asset_service,
                                           required_sessions=required_sessions)
    else:
        data = await backfill_sid_data(data=data, asset_service=asset_service,
                                             required_sessions=required_sessions)
    return data


async def backfill_sid_data(data: pl.DataFrame, asset_service: AssetService, required_sessions: pl.Series):
    unique_symbols = list(data["symbol"].unique())
    symbol_to_sid = {a.symbol: a.sid for a in
                     await asset_service.get_exchange_equities_by_symbols(
                         symbols=[AssetSymbol(symbol=symbol, mic="XNGS") for symbol in unique_symbols]
                     )}
    required_dates = (
        required_sessions["date"]
        if isinstance(required_sessions, pl.DataFrame)
        else required_sessions
    ).unique()
    expected_rows = pl.DataFrame({"symbol": unique_symbols}).join(
        pl.DataFrame({"date": required_dates}),
        how="cross",
    )
    missing_rows = expected_rows.join(
        data.select(["symbol", "date"]).unique(),
        on=["symbol", "date"],
        how="anti",
    )

    for symbol in missing_rows["symbol"].unique():
        missing_dates = sorted(
            missing_rows.filter(pl.col("symbol") == symbol)["date"].to_list()
        )
        _logger.warning(
            f"Data for symbol {symbol} is missing on ticks "
            f"({len(missing_dates)}): {[date.isoformat() for date in missing_dates]}"
        )

    missing_symbols = set(unique_symbols) - set(symbol_to_sid)
    if missing_symbols:
        raise ValueError(f"Symbols are missing in asset database: {missing_symbols}")
    if not missing_rows.is_empty():
        data = pl.concat(
            [
                data,
                missing_rows.cast(
                    {"symbol": data.schema["symbol"], "date": data.schema["date"]}
                ),
            ],
            how="diagonal",
        )
    data = data.with_columns(
        pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64).alias("sid")
    ).sort(["sid", "date"])
    return data


async def _backfill_symbol_data(
        data: pl.DataFrame,
        asset_service: AssetService,
        required_sessions: pl.DataFrame | pl.Series,
) -> pl.DataFrame:
    """Add missing sessions to data that is already keyed by ``sid``.

    Backfilling repairs the shape of a bundle only: rows created for a missing
    session contain the key columns and null values for all source fields.
    """
    if "sid" not in data.columns or "date" not in data.columns:
        raise ValueError("Data keyed by sid must contain 'sid' and 'date' columns.")

    session_values = (
        required_sessions["date"]
        if isinstance(required_sessions, pl.DataFrame)
        else required_sessions
    )
    required_dates = set(session_values.to_list())
    sids = data["sid"].drop_nulls().unique().to_list()
    if not sids:
        return data

    assets = await asset_service.get_assets_by_ids(ids=[int(sid) for sid in sids])
    resolved_sids = {asset.sid for asset in assets if asset is not None}
    missing_sids = set(sids) - resolved_sids
    if missing_sids:
        raise ValueError(f"SIDs are missing in asset database: {sorted(missing_sids)}")

    additions: list[pl.DataFrame] = []
    for sid in sids:
        existing_dates = set(
            data.filter(pl.col("sid") == sid)["date"].drop_nulls().to_list()
        )
        missing_dates = sorted(required_dates - existing_dates)
        if not missing_dates:
            continue
        _logger.warning(
            "Data is missing sessions",
            sid=sid,
            count=len(missing_dates),
            dates=[date.isoformat() for date in missing_dates],
        )
        additions.append(
            pl.DataFrame(
                {"sid": [sid] * len(missing_dates), "date": missing_dates},
                schema_overrides={"sid": data.schema["sid"], "date": data.schema["date"]},
            )
        )

    if additions:
        data = pl.concat([data, *additions], how="diagonal")
    return data.sort(["sid", "date"])
