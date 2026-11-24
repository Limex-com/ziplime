"""Utils for market."""

import datetime
import numpy as np
import pandas as pd
import polars as pl
from exchange_calendars import ExchangeCalendar
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.sweep.params import SweepError
from ziplime.assets.services.asset_service import AssetService
from ziplime.sweep.market.support import BEFORE_TRADING_START
from ziplime.sweep.market.dividend import Dividend
from ziplime.sweep.market.support import FIELDS
from ziplime.sweep.market.market import Market
from ziplime.sweep.market.date_utils import _date
from ziplime.sweep.market.session_utils import _positions
from ziplime.sweep.market.session_utils import bar_grid
from ziplime.sweep.market.session_utils import run_sessions


async def build_market(bundle_rows: pl.DataFrame, universe: dict, calendar: ExchangeCalendar,
                       start: datetime.date, end: datetime.date, asset_service: AssetService,
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


async def corporate_actions(asset_service: AssetService, listings: list, sessions: pd.DatetimeIndex):
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
