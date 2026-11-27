"""Datetime param for cli."""

import datetime
import asyncclick as click
from ziplime.utils.cli_utils import _coerce_tz


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
