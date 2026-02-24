import polars as pl
import datetime

import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period

_logger = structlog.get_logger(__name__)
async def _process_data(data: pl.DataFrame,
                        date_start: datetime.datetime,
                        date_end: datetime.datetime,
                        data_frequency_use_window_end: bool,
                        asset_service: AssetService, name: str, symbols: list[str],
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
        data = await _backfill_sid_data(data=data, asset_service=asset_service,
                                             required_sessions=required_sessions)
    return data


async def _backfill_sid_data(data: pl.DataFrame, asset_service: AssetService, required_sessions: pl.Series):
    unique_symbols = list(data["symbol"].unique())
    symbol_to_sid = {a.get_symbol_by_exchange(exchange_name=None): a.sid for a in
                     await asset_service.get_equities_by_symbols(unique_symbols)}
    data = data.with_columns(
        pl.lit(0).alias("sid"),
    )

    for symbol in unique_symbols:
        symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
        missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))

        if len(missing_sessions) > 0:
            _logger.warning(
                f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
            new_rows_df = pl.DataFrame(
                {"date": missing_sessions, "symbol": symbol},
                schema_overrides={"date": data.schema["date"]}
            )
            # Concatenate with the original DataFrame
            data = pl.concat([data, new_rows_df], how="diagonal")

        data = data.with_columns(
            pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64).alias("sid")
        ).sort(["sid", "date"])
    return data


async def _backfill_symbol_data(self):
    pass
