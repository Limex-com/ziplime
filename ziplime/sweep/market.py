"""The market as the engine sees it, bar by bar, prepared once on the host in float64.

Everything the engine reads at a bar comes from the bundle's rows through two questions:

* the latest row at or before a moment (``get_spot_value``: ``date <= dt``, last row) -- what
  ``data.current``, the fill step, sizing and the portfolio marks read;
* the rows strictly before a moment (``get_data_by_limit(include_end_date=False)``) -- what
  ``data.history`` reads.

Answering them per instrument and per session once, here, is what lets the simulation run as
array code. A session with no row for an instrument therefore reads the previous row -- stale, as
in the engine -- and whether ``data.history`` includes the current bar depends, as in the engine,
on how the bundle stamps its bars: rows stamped at the session close end the window at the previous
bar, rows stamped at midnight include the current one.
"""
import dataclasses
import datetime

import numpy as np
import pandas as pd
import polars as pl

from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.sweep.params import SweepError

#: Fields a bundle row may carry that the sweep reads.
FIELDS = ("open", "high", "low", "close", "price", "volume")

#: before_trading_start runs this long before the session's first minute.
BEFORE_TRADING_START = datetime.timedelta(minutes=46)


@dataclasses.dataclass
class Dividend:
    column: int
    ex_bar: int
    pay_bar: int
    amount: float


@dataclasses.dataclass
class Market:
    #: Session closes of the run, in the calendar's zone: the engine's bar timestamps
    index: pd.DatetimeIndex
    columns: list[str]
    listings: list
    #: {field: (bars, instruments)} -- the latest row at or before the bar
    spot: dict
    #: The same at before_trading_start
    spot_before: dict
    #: (bars, instruments): price the portfolio is marked at (previous mark where none)
    marks: np.ndarray
    #: (bars, instruments): rows of each instrument before the bar / before before_trading_start
    history_end: np.ndarray
    history_end_before: np.ndarray
    #: {field: (rows, instruments)}: each instrument's own rows, NaN-padded at the end
    rows: dict
    #: (bars, instruments): orders refused -- the session is past the instrument's last bar or
    #: its auto-close date
    stopped: np.ndarray
    #: (bars, instruments): the engine closes the position at this bar's session end
    auto_close: np.ndarray
    #: (instruments,): contract multiplier; futures are valued by their margin flows
    multiplier: np.ndarray
    is_future: np.ndarray
    #: (bars, instruments): split ratio (old/new shares) effective at the bar, 1.0 for none
    splits: np.ndarray
    dividends: list
    #: (bars,): the session's first bar (dividends, splits, before_trading_start come before it)
    #: and its last (auto-close, the cancel policy and the day's portfolio value come after it)
    session_start: np.ndarray = None
    session_end: np.ndarray = None
    #: The emission rate: one day, or a part of one
    rate: datetime.timedelta = datetime.timedelta(days=1)
    #: (rows, instruments): each row's local calendar day, as a day number; -1 past the rows
    row_days: np.ndarray = None

    @property
    def n_bars(self) -> int:
        return len(self.index)

    @property
    def intraday(self) -> bool:
        return self.rate < datetime.timedelta(days=1)

    @property
    def session_index(self) -> pd.DatetimeIndex:
        """The bars that end a session: where the engine records the day's portfolio value."""
        return self.index[self.session_end]

    @property
    def has_splits(self) -> bool:
        return bool((self.splits != 1.0).any())

    @property
    def has_futures(self) -> bool:
        return bool(self.is_future.any())

    @property
    def has_auto_close(self) -> bool:
        return bool(self.auto_close.any())


def parse_rate(value) -> datetime.timedelta:
    """An emission rate or history frequency: a timedelta, or "1m", "5m", "1h", "1d", "1min"..."""
    if value is None:
        return datetime.timedelta(days=1)
    if isinstance(value, datetime.timedelta):
        return value
    text = str(getattr(value, "value", value)).strip().lower()
    for suffix, unit in (("min", "minutes"), ("m", "minutes"), ("h", "hours"), ("d", "days")):
        if text.endswith(suffix) and text[:-len(suffix)].strip().isdigit():
            return datetime.timedelta(**{unit: int(text[:-len(suffix)])})
    try:
        return pd.Timedelta(text).to_pytimedelta()
    except ValueError:
        raise SweepError(f"Not a bar frequency: {value!r} (use 1m, 5m, 1h, 1d).") from None


def run_sessions(calendar, start: datetime.date, end: datetime.date) -> pd.DatetimeIndex:
    return calendar.sessions_in_range(start, end)


def _positions(stamps: np.ndarray, moments: np.ndarray, side: str) -> np.ndarray:
    return np.searchsorted(stamps, moments, side=side)


def bar_grid(calendar, sessions: pd.DatetimeIndex, rate: datetime.timedelta):
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


async def build_market(bundle_rows: pl.DataFrame, universe: dict, calendar,
                       start: datetime.date, end: datetime.date, asset_service,
                       rate: datetime.timedelta = datetime.timedelta(days=1)) -> Market:
    """The run's bars from ``start`` to ``end`` and everything the engine reads at each."""
    sessions = run_sessions(calendar, start, end)
    if len(sessions) == 0:
        raise SweepError(f"No {calendar.name} sessions between {start} and {end}.")
    bars, session_start, session_end, session_of = bar_grid(calendar, sessions, rate)
    # before_trading_start: 46 minutes before the first minute. Intraday the engine reads data
    # there as of the previous market minute -- the previous session's close.
    befores = []
    for session in sessions:
        moment = calendar.session_first_minute(session) - BEFORE_TRADING_START
        if rate < datetime.timedelta(days=1):
            moment = calendar.previous_minute(moment)
        befores.append(moment)
    befores = pd.DatetimeIndex(befores).tz_convert(calendar.tz)[session_of]
    bar_ns = bars.tz_convert("UTC").asi8
    before_ns = befores.tz_convert("UTC").asi8

    names, listings = list(universe), list(universe.values())
    n_bars, n_columns = len(bars), len(listings)
    data = bundle_rows.sort(["sid", "date"])
    fields = [field for field in FIELDS if field in data.columns]
    per_sid = {sid: frame for (sid,), frame in data.partition_by("sid", as_dict=True).items()}
    longest = max((len(per_sid.get(listing.sid, ())) for listing in listings), default=0)

    rows = {field: np.full((longest, n_columns), np.nan) for field in fields}
    row_days = np.full((longest, n_columns), -1, dtype=np.int64)
    spot = {field: np.full((n_bars, n_columns), np.nan) for field in fields}
    spot_before = {field: np.full((n_bars, n_columns), np.nan) for field in fields}
    history_end = np.zeros((n_bars, n_columns), dtype=np.int64)
    history_end_before = np.zeros((n_bars, n_columns), dtype=np.int64)
    stopped = np.zeros((n_bars, n_columns), dtype=bool)
    auto_close = np.zeros((n_bars, n_columns), dtype=bool)
    session_dates = np.array([s.date() for s in sessions])
    bar_dates = session_dates[session_of]
    last_bar = np.nonzero(session_end)[0]  # each session's last bar

    for column, listing in enumerate(listings):
        frame = per_sid.get(listing.sid)
        if frame is None or frame.is_empty():
            stopped[:, column] = True
            continue
        stamps = frame["date"].dt.convert_time_zone("UTC").dt.timestamp("ns").to_numpy()
        local = frame["date"].dt.convert_time_zone(str(calendar.tz)).dt.date()
        row_days[:len(local), column] = local.cast(pl.Int32).to_numpy()
        for field in fields:
            values = frame[field].cast(pl.Float64).to_numpy()
            rows[field][:len(values), column] = values
            at = _positions(stamps, bar_ns, "right") - 1
            spot[field][:, column] = np.where(at >= 0, values[np.maximum(at, 0)], np.nan)
            at = _positions(stamps, before_ns, "right") - 1
            spot_before[field][:, column] = np.where(at >= 0, values[np.maximum(at, 0)], np.nan)
        history_end[:, column] = _positions(stamps, bar_ns, "left")
        history_end_before[:, column] = _positions(stamps, before_ns, "left")
        # has_stopped_trading: a session after the instrument's last bar
        last_session = local.max()
        closing = getattr(listing, "auto_close_date", None) or getattr(listing.asset, "auto_close_date", None)
        past_end = session_dates > last_session
        past_close = session_dates > closing if closing else np.zeros(len(sessions), dtype=bool)
        stopped[:, column] = (past_end | past_close)[session_of]
        # Closed at the end of the first session past its bars, or of its auto-close date
        if past_end.any():
            auto_close[last_bar[int(np.argmax(past_end))], column] = True
        if closing:
            at_close = session_dates >= closing
            if at_close.any():
                auto_close[last_bar[int(np.argmax(at_close))], column] = True

    marks = pd.DataFrame(spot.get("close", spot.get("price"))).ffill().to_numpy()
    multiplier = np.array([float(getattr(listing.asset, "multiplier", 1.0) or 1.0)
                           if isinstance(listing.asset, FuturesContract) else 1.0
                           for listing in listings])
    is_future = np.array([isinstance(listing.asset, FuturesContract) for listing in listings])
    session_splits, dividends = await corporate_actions(asset_service, listings, sessions)
    # Corporate actions happen at the session's start: on its first bar
    first_bar = np.nonzero(session_start)[0]
    splits = np.ones((n_bars, n_columns))
    splits[first_bar] = session_splits
    for dividend in dividends:
        dividend.ex_bar, dividend.pay_bar = first_bar[dividend.ex_bar], first_bar[dividend.pay_bar]
    del bar_dates
    return Market(index=bars, columns=names, listings=listings, spot=spot,
                  spot_before=spot_before, marks=marks, history_end=history_end,
                  history_end_before=history_end_before, rows=rows, stopped=stopped,
                  auto_close=auto_close, multiplier=multiplier, is_future=is_future,
                  splits=splits, dividends=dividends, session_start=session_start,
                  session_end=session_end, rate=rate, row_days=row_days)


async def corporate_actions(asset_service, listings: list, sessions: pd.DatetimeIndex):
    """Splits and cash dividends of the universe over the run, from the asset database the engine
    reads them from, placed on the run's sessions the way the engine matches them: a split on its
    effective date, a dividend earned on its ex date and paid on its pay date -- each only if that
    date is a session of the run."""
    n_bars, n_columns = len(sessions), len(listings)
    splits = np.ones((n_bars, n_columns))
    dividends = []
    if asset_service is None:
        return splits, dividends
    bar_of = {session.date(): k for k, session in enumerate(sessions)}
    column_of = {listing.asset.id: k for k, listing in enumerate(listings)
                 if getattr(listing.asset, "id", None) is not None}
    assets = [listing.asset for listing in listings]
    first, last = sessions[0].date(), sessions[-1].date()
    try:
        found_splits = await asset_service.get_splits_by_assets_and_effective_date_between(
            assets=assets, effective_date_from=first, effective_date_to=last)
        found_dividends = await asset_service.get_dividends_by_assets_and_ex_date_between(
            assets=assets, ex_date_from=first, ex_date_to=last)
    except AttributeError:
        return splits, dividends
    for split in found_splits or []:
        column, bar = column_of.get(split.asset.id), bar_of.get(_date(split.effective_date))
        if column is not None and bar is not None:
            splits[bar, column] = float(split.ratio)
    for dividend in found_dividends or []:
        column = column_of.get(dividend.asset.id)
        ex_bar = bar_of.get(_date(dividend.ex_date))
        pay_bar = bar_of.get(_date(dividend.pay_date)) if dividend.pay_date else None
        # The engine pays on the pay date's session only: a pay date that is not a session of the
        # run is never paid
        if column is not None and ex_bar is not None and pay_bar is not None and pay_bar >= ex_bar:
            dividends.append(Dividend(column=column, ex_bar=ex_bar, pay_bar=pay_bar,
                                      amount=float(dividend.amount)))
    return splits, dividends


def _date(value):
    return value.date() if isinstance(value, datetime.datetime) else value


def asof_rows(panel_index: pd.DatetimeIndex, bars: pd.DatetimeIndex) -> np.ndarray:
    """For each bar, the last panel row at or before it -- how SignalPanel reads a signal on a bar
    it has no row for. -1 where there is none."""
    stamps = panel_index.tz_convert("UTC").asi8 if panel_index.tz else panel_index.asi8
    return np.searchsorted(stamps, bars.tz_convert("UTC").asi8, side="right") - 1


# -- data.history windows ----------------------------------------------------------------------------

#: Window statistics the sweep provides, as polars computes them on the rows history returns.
WINDOW_OPS = ("mean", "sum", "std", "var", "min", "max", "median", "first", "last", "count")


#: How a coarser bar is built from finer rows (DataBundle.get_data_by_limit's resampling).
DAILY_AGGREGATE = {"open": "first", "high": "max", "low": "min", "volume": "sum"}


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


class HistoryRows:
    """The rows ``data.history(assets=[asset], bar_count=n, fields=[field])[field]`` holds at a
    bar: what a strategy's own code reads from history (``.to_numpy()``), for the host."""

    def __init__(self, market: Market):
        self.market = market
        self._daily = {}

    def window(self, field: str, column: int, t: int, length: int, before: bool,
               daily: bool) -> np.ndarray:
        market = self.market
        if field not in market.rows:
            raise SweepError(f"The bundle has no {field!r} column.")
        length = int(length)
        if length < 1:
            raise SweepError(f"history bar_count has to be at least 1, got {length}.")
        end = int((market.history_end_before if before else market.history_end)[t, column])
        if end <= 0:
            return np.empty(0)
        if not daily:
            return market.rows[field][max(end - length, 0):end, column].copy()
        key = (field, column)
        if key not in self._daily:
            self._daily[key] = _daily_parts(market, field, column)
        daily_rows, partial, day_index = self._daily[key]
        last = end - 1
        d = day_index[last]
        return np.append(daily_rows[max(0, d - length + 1):d], partial[last])


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
