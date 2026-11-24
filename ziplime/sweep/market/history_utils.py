"""History utils for market."""

import numpy as np
import pandas as pd
from ziplime.sweep.params import SweepError
from ziplime.sweep.market.support import DAILY_AGGREGATE
from ziplime.sweep.market.market import Market


def asof_rows(panel_index: pd.DatetimeIndex, bars: pd.DatetimeIndex) -> np.ndarray:
    """For each bar, the last panel row at or before it -- how SignalPanel reads a signal on a bar
    it has no row for. -1 where there is none."""
    stamps = panel_index.tz_convert("UTC").asi8 if panel_index.tz else panel_index.asi8
    return np.searchsorted(stamps, bars.tz_convert("UTC").asi8, side="right") - 1


def _daily_parts(market: Market, field: str, column: int):
    """One instrument's rows grouped into calendar days: (whole days, each row's day so far,
    each row's day number), aggregated as OHLCV; None without rows."""
    values, days = market.rows[field], market.row_days
    how = DAILY_AGGREGATE.get(field, "last")
    present = days[:, column] >= 0
    row_values, row_days = values[present, column], days[present, column]
    if not len(row_days):
        return None
    frame = pd.DataFrame({"day": row_days, "value": row_values})
    grouped = frame.groupby("day", sort=True)["value"]
    full = getattr(grouped, how)()                      # each whole day
    partial = getattr(grouped, {"first": "cummax", "max": "cummax", "min": "cummin",
                                "sum": "cumsum", "last": "cummax"}[how])() \
        if how not in ("first", "last") else None
    day_index = np.searchsorted(full.index.to_numpy(), row_days)
    if how == "first":
        partial = full.to_numpy()[day_index]
    elif how == "last":
        partial = row_values
    else:
        partial = partial.to_numpy()
    return full.to_numpy(), partial, day_index


def daily_window_values(market: Market, field: str, op: str, length: int,
                        before: bool) -> np.ndarray:
    """``data.history(..., frequency=1 day)`` on an intraday run: the rows before the bar grouped
    into calendar days -- today's partial one included -- each day aggregated as OHLCV, the last
    ``length`` of them, and ``op`` over those."""
    ends = market.history_end_before if before else market.history_end
    result = np.full(ends.shape, np.nan)
    for column in range(ends.shape[1]):
        parts = _daily_parts(market, field, column)
        if parts is None:
            continue
        daily, partial, day_index = parts
        for t, end in enumerate(ends[:, column]):
            if end <= 0:
                continue
            last = end - 1
            d = day_index[last]
            window = np.append(daily[max(0, d - length + 1):d], partial[last])
            result[t, column] = _statistic(window, op)
    return result


def _statistic(window: np.ndarray, op: str) -> float:
    present = window[~np.isnan(window)]
    if op == "count":
        return float(len(window))
    if op == "first":
        return window[0]
    if op == "last":
        return window[-1]
    if not len(present):
        return np.nan
    if op in ("std", "var"):
        if len(present) < 2:
            return np.nan
        return float(np.std(present, ddof=1) if op == "std" else np.var(present, ddof=1))
    return float({"mean": np.mean, "sum": np.sum, "min": np.min, "max": np.max,
                  "median": np.median}[op](present))


def window_values(market: Market, field: str, op: str, length: int, before: bool) -> np.ndarray:
    """(bars, instruments): ``op`` over each instrument's last ``length`` rows before the bar.

    Up to ``length`` rows per instrument, fewer where it has less history, nulls skipped by the
    statistics and counted by ``count`` -- what ``data.history(...)[field].<op>()`` returns."""
    if field not in market.rows:
        raise SweepError(f"The bundle has no {field!r} column.")
    values = market.rows[field]
    ends = market.history_end_before if before else market.history_end
    length = int(length)
    if length < 1:
        raise SweepError(f"history bar_count has to be at least 1, got {length}.")
    frame = pd.DataFrame(values)
    if op in ("first", "last", "count"):
        rolled = None
    else:
        window = frame.rolling(length, min_periods=1)
        rolled = getattr(window, op)().to_numpy() if op not in ("std", "var") else \
            getattr(window, op)(ddof=1).to_numpy()
    result = np.full(ends.shape, np.nan)
    for column in range(values.shape[1]):
        end = ends[:, column]
        has = end > 0
        last = np.maximum(end - 1, 0)
        if op == "last":
            result[:, column] = np.where(has, values[last, column], np.nan)
        elif op == "first":
            result[:, column] = np.where(has, values[np.maximum(end - length, 0), column], np.nan)
        elif op == "count":
            result[:, column] = np.minimum(end, length)
        else:
            result[:, column] = np.where(has, rolled[last, column], np.nan)
    return result
