"""Date for cli."""

import datetime
from ziplime.utils.cli.datetime_param import _DatetimeParam


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
