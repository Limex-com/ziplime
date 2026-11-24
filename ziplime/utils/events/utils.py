"""Utils for events."""

import functools
import warnings
import datetime
from collections.abc import Callable
import pytz


def ensure_utc(time: datetime.datetime | datetime.time, tz: str = "UTC"):
    """Normalize a time. If the time is tz-naive, assume it is UTC."""
    if not time.tzinfo:
        time = time.replace(tzinfo=pytz.timezone(tz))
    return time.replace(tzinfo=pytz.utc)


def _out_of_range_error(a: int, b: int | None = None, var: str = "offset"):
    start = 0
    if b is None:
        end = a - 1
    else:
        start = a
        end = b - 1
    return ValueError(
        "{var} must be in between {start} and {end} inclusive".format(
            var=var,
            start=start,
            end=end,
        )
    )


def _td_check(td: datetime.timedelta):
    seconds = td.total_seconds()

    # 43200 seconds = 12 hours
    if 60 <= seconds <= 43200:
        return td
    else:
        raise ValueError(
            "offset must be in between 1 minute and 12 hours, " "inclusive."
        )


def _build_offset(
    offset: datetime.timedelta | None,
    kwargs: dict[str, float | None],
    default: datetime.timedelta | None,
):
    """
    Builds the offset argument for event rules.
    """
    # Filter down to just kwargs that were actually passed.
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    if offset is None:
        if not kwargs:
            return default  # use the default.
        else:
            return _td_check(datetime.timedelta(**kwargs))
    elif kwargs:
        raise ValueError("Cannot pass kwargs and an offset")
    elif isinstance(offset, datetime.timedelta):
        return _td_check(offset)
    else:
        raise TypeError("Must pass 'hours' and/or 'minutes' as keywords")


def _build_date(date: datetime.date | None, kwargs: dict[str, int]):
    """
    Builds the date argument for event rules.
    """
    if date is None:
        if not kwargs:
            raise ValueError("Must pass a date or kwargs")
        else:
            return datetime.date(**kwargs)

    elif kwargs:
        raise ValueError("Cannot pass kwargs and a date")
    else:
        return date


def lossless_float_to_int(funcname: str, argname: str, arg: float):
    """Coerce an integral float to an int, and refuse a fractional one.

    ``date_rules.month_start(days_offset=1.0)`` means the second trading day; ``1.5`` means
    nothing, and silently truncating it would schedule the caller's function on a day they did not
    ask for. Upstream applied this through a ``@preprocess`` decorator, which this fork does not
    carry -- it is called directly instead.
    """
    if not isinstance(arg, float):
        return arg

    arg_as_int = int(arg)
    if arg == arg_as_int:
        warnings.warn(
            "{f} expected an int for argument {name!r}, but got float {arg}."
            " Coercing to int.".format(
                f=funcname,
                name=argname,
                arg=arg,
            ),
        )
        return arg_as_int

    raise TypeError(arg)


def _per_calendar(compute: Callable):
    """Memoise a rule property against the calendar it was computed for.

    ``execution_period_values`` groups every session the calendar knows -- 36 410 of them for
    XNYS -- and was recomputing that on **every bar**, 2.2 ms a time, because upstream's
    ``@lazyval`` did not survive the port into this fork. Keying the cache on the calendar rather
    than caching outright keeps the ``cal`` setter meaningful: threading a different calendar
    through a rule recomputes rather than silently answering for the old one.
    """
    name = f"_{compute.__name__}_for"

    @property
    @functools.wraps(compute)
    def cached(self):
        entry = getattr(self, name, None)
        if entry is not None and entry[0] is self.cal:
            return entry[1]
        value = compute(self)
        setattr(self, name, (self.cal, value))
        return value

    return cached
