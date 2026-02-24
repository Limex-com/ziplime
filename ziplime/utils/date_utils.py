import datetime

from toolz import partition_all

from ziplime.constants.period import Period


def compute_date_range_chunks(sessions, start_date, end_date, chunksize):
    """Compute the start and end dates to run a pipeline for.

    Parameters
    ----------
    sessions : DatetimeIndex
        The available dates.
    start_date : pd.Timestamp
        The first date in the pipeline.
    end_date : pd.Timestamp
        The last date in the pipeline.
    chunksize : int or None
        The size of the chunks to run. Setting this to None returns one chunk.

    Returns
    -------
    ranges : iterable[(np.datetime64, np.datetime64)]
        A sequence of start and end dates to run the pipeline for.
    """
    if start_date not in sessions:
        raise KeyError(
            "Start date %s is not found in calendar."
            % (start_date.strftime("%Y-%m-%d"),)
        )
    if end_date not in sessions:
        raise KeyError(
            "End date %s is not found in calendar." % (end_date.strftime("%Y-%m-%d"),)
        )
    if end_date < start_date:
        raise ValueError(
            "End date %s cannot precede start date %s."
            % (end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d"))
        )

    if chunksize is None:
        return [(start_date, end_date)]

    start_ix, end_ix = sessions.slice_locs(start_date, end_date)
    return ((r[0], r[-1]) for r in partition_all(chunksize, sessions[start_ix:end_ix]))


def make_utc_aware(dti):
    """Normalizes a pd.DateTimeIndex. Assumes UTC if tz-naive."""
    try:
        # ensure tz-aware Timestamp has tz UTC
        return dti.tz_convert(tz="UTC")
    except TypeError:
        # if naive, instead convert timestamp to UTC
        return dti.tz_localize(tz="UTC")


def strip_time_and_timezone_info(dt: datetime.datetime) -> datetime.datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def period_to_timedelta(period: Period | datetime.timedelta) -> datetime.timedelta:
    if type(period) is datetime.timedelta:
        return period
    if period == "1us":
        return datetime.timedelta(microseconds=1)
    if period == "1ms":
        return datetime.timedelta(milliseconds=1)
    if period == "1s":
        return datetime.timedelta(seconds=1)
    if period == "1m":
        return datetime.timedelta(minutes=1)
    if period == "1h":
        return datetime.timedelta(hours=1)
    if period == "1d":
        return datetime.timedelta(days=1)
    if period == "1w":
        return datetime.timedelta(days=7)
    if period == "1mo":
        return datetime.timedelta(days=30)
    if period == "1q":
        return datetime.timedelta(days=90)
    if period == "1y":
        return datetime.timedelta(days=365)

    raise ValueError(f"Invalid period: {period}")
