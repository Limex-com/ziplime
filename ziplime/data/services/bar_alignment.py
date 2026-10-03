"""Putting a vendor's intraday bars on the simulation clock's grid, without a look-ahead.

Price vendors label an intraday bar with the time it **starts**. Yahoo's 09:30 five-minute bar
covers 09:30 to 09:35, and its close is not knowable until 09:35. The simulation clock emits the
minute a bar **ends** -- 09:31 for the first minute of an XNYS session, 16:00 for the last -- so
the two have to be reconciled before a bundle is assembled.

Getting it backwards is a look-ahead of exactly one bar, and it does not announce itself: the run
completes, no exception is raised, and the returns are simply better than they should be. On a
five-minute grid it hands the algorithm five minutes of the future in every price it reads.

So a bar starting at ``t`` is stamped at **the first clock minute at or after ``t + bar length``**
-- the first moment its close was knowable. On a one-minute grid that is ``t + 1min`` exactly. On
coarser grids the clock's minutes fall at 09:31, 09:36, 09:41 and so on, so the 09:35 bar closes
at 09:40 and is stamped 09:41.

The last bar of each session is dropped by that rule. Yahoo's 15:55 five-minute bar closes at
16:00 and there is no clock minute after it inside the session, so its close cannot be traded on
that day. Dropping it is the correct answer rather than a rounding loss -- but it does mean a
strategy that wants to act near the close should schedule on ``time_rules.market_close`` with an
offset of at least one bar and use the bar before it.
"""
import datetime

import polars as pl
from exchange_calendars import ExchangeCalendar


def clock_minutes(calendar: ExchangeCalendar, start: datetime.date, end: datetime.date,
                  emission_rate: datetime.timedelta) -> pl.Series:
    """The minutes an intraday simulation clock emits between two dates.

    Built the way :class:`~ziplime.gens.domain.simulation_clock.SimulationClock` builds them, from
    each session's first minute to its close, so a bundle can be stamped on exactly the instants
    the simulation will ask about.
    """
    sessions = calendar.sessions_in_range(start, end)
    opens = calendar.first_minutes.loc[sessions].dt.tz_convert(calendar.tz)
    closes = calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz)
    return pl.concat([pl.datetime_range(open_at, close_at, interval=emission_rate, eager=True)
                      for open_at, close_at in zip(opens, closes)]).sort()


def align_to_clock(bars: pl.DataFrame, grid: pl.Series, emission_rate: datetime.timedelta,
                   column: str = "date") -> pl.DataFrame:
    """Restamp start-labelled bars at the first clock minute their close was knowable.

    Args:
        bars: Frame carrying ``column``, the time each bar **starts**.
        grid: The clock's minutes, sorted -- see :func:`clock_minutes`.
        emission_rate: How long one bar covers.
        column: Which column holds the bar's start time.

    Returns:
        ``bars`` with ``column`` moved onto the grid. Bars whose close falls after the last clock
        minute -- the final one of each session -- are dropped rather than pulled backwards onto a
        minute that precedes them, which would be the look-ahead this function exists to prevent.
    """
    knowable_at = bars[column] + emission_rate
    positions = grid.search_sorted(knowable_at, side="left")
    within = positions < grid.len()
    stamped = grid.gather(positions.set(~within, 0))
    return (bars.with_columns(stamped.alias(column), within.alias("_within_session"))
            .filter(pl.col("_within_session")).drop("_within_session"))
