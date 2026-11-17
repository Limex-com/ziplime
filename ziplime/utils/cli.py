import datetime
from zoneinfo import ZoneInfo

import asyncclick as click


def _coerce_tz(tz: str | datetime.tzinfo | None):
    if tz is None or isinstance(tz, datetime.tzinfo):
        return tz
    if isinstance(tz, str):
        return ZoneInfo(tz)
    return tz


class _DatetimeParam(click.ParamType):
    def __init__(self, tz: str | datetime.tzinfo | None = None) -> None:
        self.tz = _coerce_tz(tz)

    def parser(self, value: str | datetime.date) -> datetime.datetime:
        if isinstance(value, datetime.datetime):
            dt = value
        elif isinstance(value, datetime.date):
            dt = datetime.datetime(value.year, value.month, value.day)
        elif isinstance(value, str):
            dt = datetime.datetime.fromisoformat(value)
        else:
            raise TypeError(f"Cannot parse {type(value)} into datetime")

        if self.tz is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=self.tz)
            else:
                dt = dt.astimezone(self.tz)
        return dt

    @property
    def name(self) -> str:
        return type(self).__name__.upper()

    def convert(self, value: str | datetime.date, param: click.Parameter | None, ctx: click.Context | None):
        try:
            return self.parser(value)
        except (ValueError, TypeError):
            self.fail(
                "%s is not a valid %s" % (value, self.name.lower()),
                param,
                ctx,
            )


class Timestamp(_DatetimeParam):
    """A click parameter that parses the value into datetime.datetime objects.

    Parameters
    ----------
    tz : timezone-coercable, optional
        The timezone to parse the string as.
        By default the timezone will be inferred from the string or naive.
    """


class Date(_DatetimeParam):
    """A click parameter that parses the value into datetime.date objects.

    Parameters
    ----------
    tz : timezone-coercable, optional
        The timezone to parse the string as.
        By default the timezone will be inferred from the string or naive.
    as_timestamp : bool, optional
        If True, return the value as a datetime.datetime object normalized to
        midnight.
    """

    def __init__(self, tz: str | datetime.tzinfo | None = None, as_timestamp: bool = False) -> None:
        super(Date, self).__init__(tz=tz)
        self.as_timestamp = as_timestamp

    def parser(self, value: str | datetime.date) -> datetime.date | datetime.datetime:
        ts = super(Date, self).parser(value)
        return ts.replace(hour=0, minute=0, second=0, microsecond=0) if self.as_timestamp else ts.date()
