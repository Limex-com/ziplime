"""Timestamp for cli."""

from ziplime.utils.cli.datetime_param import _DatetimeParam


class Timestamp(_DatetimeParam):
    """A click parameter that parses the value into datetime.datetime objects.

    Parameters
    ----------
    tz : timezone-coercable, optional
        The timezone to parse the string as.
        By default the timezone will be inferred from the string or naive.
    """
