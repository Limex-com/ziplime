"""Time utils for huggingface_data_source."""

import datetime
from zoneinfo import ZoneInfo
import polars as pl
from ziplime.data.data_sources.huggingface.manifest import ManifestError
from ziplime.utils.date_utils import normalize_datetime


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    """The calendar day of a bound, whichever of the two types it arrived as."""
    return value.date() if isinstance(value, datetime.datetime) else value


def _at_zone(day: datetime.date | datetime.datetime, timezone: str,
             end_of_day: bool = False) -> datetime.datetime:
    """A window bound as an instant in ``timezone``, matching the mounted date column."""
    zone = ZoneInfo(timezone)
    if isinstance(day, datetime.datetime):
        return normalize_datetime(day, zone)
    return normalize_datetime(
        datetime.datetime.combine(
            day, datetime.time.max if end_of_day else datetime.time.min
        ),
        zone,
    )


def _to_session_time(column: str, dtype: pl.DataType, timezone: str) -> pl.Expr:
    """Place a date column on the simulation's clock, whatever it was published as.

    The dtype comes from the scan's schema rather than being tested per row: the three shapes seen
    in practice are decided once for the whole column.

    * A plain ``Date`` -- congress ``notification_date``. It names a **calendar day**, not an
      instant, so it is placed at midnight *in the session's own timezone*. Reading it as midnight
      UTC and converting would move it to the previous evening anywhere west of Greenwich: a
      disclosure dated the 24th became visible at 20:00 on the 23rd, four hours of look-ahead on
      the wrong trading session.
    * A timestamp carrying a zone -- insider ``feature_available_at``. Converted; the instant is
      unchanged and only its label moves.
    * A naive timestamp -- read **as** UTC rather than as local time. Guessing the publisher's
      timezone would shift every row by hours, and these datasets stamp in UTC.
    """
    expression = pl.col(column)
    if dtype == pl.Date:
        return expression.cast(pl.Datetime("us")).dt.replace_time_zone(timezone)
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            return (expression.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
                    .dt.convert_time_zone(timezone))
        return expression.dt.convert_time_zone(timezone)
    raise ManifestError(
        f"The knowledge column {column!r} is a {dtype}, which is not a date or a timestamp, so "
        f"rows cannot be placed in time.")
