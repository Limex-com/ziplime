"""Stamping a vendor's intraday bars on the simulation clock's grid.

The one piece of intraday plumbing that fails silently. A bar labelled with the time it starts,
stamped at that time, hands the algorithm the whole bar's future: on a five-minute grid every
price it reads is five minutes ahead of where the simulation thinks it is. Nothing raises, the run
completes, and the returns are better than they should be.
"""
import datetime

import polars as pl
import pytest

from ziplime.data.services.bar_alignment import align_to_clock, clock_minutes
from ziplime.utils.calendar_utils import get_calendar

CALENDAR = "XNYS"
SESSION = datetime.date(2026, 9, 2)
MINUTE = datetime.timedelta(minutes=1)
FIVE_MINUTES = datetime.timedelta(minutes=5)


def vendor_bars(emission_rate: datetime.timedelta) -> pl.DataFrame:
    """Bars as a price vendor publishes them: labelled with the time each one **starts**."""
    zone = get_calendar(CALENDAR).tz
    starts = pl.datetime_range(
        datetime.datetime(2026, 9, 2, 9, 30, tzinfo=zone),
        datetime.datetime(2026, 9, 2, 16, 0, tzinfo=zone) - emission_rate,
        interval=emission_rate, eager=True)
    return pl.DataFrame({"date": starts, "close": range(starts.len())})


def grid(emission_rate: datetime.timedelta) -> pl.Series:
    return clock_minutes(get_calendar(CALENDAR), SESSION, SESSION, emission_rate)


def test_the_clock_grid_runs_from_the_first_minute_to_the_close():
    minutes = grid(MINUTE)
    assert minutes.len() == 390
    assert minutes[0].strftime("%H:%M") == "09:31"
    assert minutes[-1].strftime("%H:%M") == "16:00"


def test_a_one_minute_bar_is_stamped_at_the_minute_it_closes():
    aligned = align_to_clock(vendor_bars(MINUTE), grid(MINUTE), MINUTE)
    assert aligned.height == 390
    assert aligned["date"][0].strftime("%H:%M") == "09:31"
    assert aligned["date"][-1].strftime("%H:%M") == "16:00"


def test_a_coarse_bar_is_stamped_at_the_first_clock_minute_after_its_close():
    """The 09:30 five-minute bar closes at 09:35; the grid's next minute is 09:36."""
    aligned = align_to_clock(vendor_bars(FIVE_MINUTES), grid(FIVE_MINUTES), FIVE_MINUTES)
    assert aligned["date"][0].strftime("%H:%M") == "09:36"


@pytest.mark.parametrize("rate", [MINUTE, FIVE_MINUTES])
def test_no_bar_is_ever_stamped_before_its_own_close(rate):
    """The whole point. A bar stamped at or before its close is a look-ahead."""
    bars = vendor_bars(rate)
    aligned = align_to_clock(bars, grid(rate), rate)
    closes_at = bars["date"].head(aligned.height) + rate
    assert (aligned["date"] >= closes_at).all()


def test_the_last_bar_of_a_session_is_dropped_rather_than_pulled_back():
    """Its close is 16:00 and the grid ends at 15:56, so there is no honest minute to trade it."""
    bars = vendor_bars(FIVE_MINUTES)
    aligned = align_to_clock(bars, grid(FIVE_MINUTES), FIVE_MINUTES)
    assert aligned.height == bars.height - 1
    assert aligned["close"].to_list() == list(range(bars.height - 1))


def test_alignment_keeps_the_other_columns():
    aligned = align_to_clock(vendor_bars(MINUTE), grid(MINUTE), MINUTE)
    assert aligned.columns == ["date", "close"]
    assert aligned["close"][0] == 0
