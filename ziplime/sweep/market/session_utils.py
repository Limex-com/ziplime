"""Session utils for market."""

import datetime
import numpy as np
import pandas as pd
from exchange_calendars import ExchangeCalendar


def run_sessions(calendar: ExchangeCalendar, start: datetime.date, end: datetime.date) -> pd.DatetimeIndex:
    return calendar.sessions_in_range(start, end)


def _positions(stamps: np.ndarray, moments: np.ndarray, side: str) -> np.ndarray:
    return np.searchsorted(stamps, moments, side=side)


def bar_grid(calendar: ExchangeCalendar, sessions: pd.DatetimeIndex, rate: datetime.timedelta):
    """The engine's bars: one per session at its close, or, intraday, from the session's first
    minute to its close every ``rate`` -- both ends included, as SimulationClock lays them out
    (a 5-minute run's bars are 09:31, 09:36, ..., 15:56)."""
    if rate >= datetime.timedelta(days=1):
        bars = pd.DatetimeIndex([calendar.session_close(s) for s in sessions]).tz_convert(calendar.tz)
        ones = np.ones(len(bars), dtype=bool)
        return bars, ones, ones, np.arange(len(bars))
    stamps, starts, ends, session_of = [], [], [], []
    for k, session in enumerate(sessions):
        first = calendar.session_first_minute(session).tz_convert(calendar.tz)
        close = calendar.session_close(session).tz_convert(calendar.tz)
        minutes = pd.date_range(first, close, freq=rate)
        stamps.extend(minutes)
        starts.extend([True] + [False] * (len(minutes) - 1))
        ends.extend([False] * (len(minutes) - 1) + [True])
        session_of.extend([k] * len(minutes))
    return (pd.DatetimeIndex(stamps), np.array(starts), np.array(ends), np.array(session_of))
