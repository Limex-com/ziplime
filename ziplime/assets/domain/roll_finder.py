"""Deciding which contract a continuous future holds on a given date.

``DataBundle`` consulted ``self._roll_finders`` but nothing ever defined the class or assigned the
attribute, so continuous futures could not resolve to a contract at all. These are the
implementations behind the two roll styles a
:class:`~ziplime.assets.domain.continuous_future.ContinuousFuture` may ask for.
"""
import bisect
import datetime

import pandas as pd
import polars as pl
import structlog

from ziplime.assets.domain.ordered_contracts import OrderedContracts
from ziplime.assets.entities.exchange_asset import ExchangeAsset


def _as_date(value: datetime.date | datetime.datetime) -> datetime.date:
    return value.date() if isinstance(value, datetime.datetime) else value


class RollFinder:
    """Base class: maps ``(root_symbol, date, offset)`` onto a contract.

    Args:
        asset_service: Used to load the contract chain of a root symbol.
        mic: Restrict chains to a single exchange, if given.
    """

    def __init__(self, asset_service, mic: str | None = None, logger=None):
        self._asset_service = asset_service
        self._mic = mic
        self._ordered_contracts: dict[str, OrderedContracts] = {}
        self._logger = logger or structlog.get_logger(__name__)

    async def get_ordered_contracts(self, root_symbol: str) -> OrderedContracts:
        """Return (and memoise) the chain of ``root_symbol``."""
        if root_symbol not in self._ordered_contracts:
            self._ordered_contracts[root_symbol] = await self._asset_service.get_ordered_contracts(
                root_symbol=root_symbol, mic=self._mic)
        return self._ordered_contracts[root_symbol]

    async def get_contract_center(self, root_symbol: str,
                                  dt: datetime.date | datetime.datetime,
                                  offset: int = 0) -> ExchangeAsset | None:
        """Return the contract a continuous future holds on ``dt`` at ``offset`` from the front."""
        raise NotImplementedError("get_contract_center")

    async def get_rolls(self, root_symbol: str,
                        start: datetime.date | datetime.datetime,
                        end: datetime.date | datetime.datetime,
                        offset: int = 0) -> list[tuple[ExchangeAsset, datetime.date | None]]:
        """Return ``[(contract, roll_date), ...]`` covering ``[start, end]``.

        ``roll_date`` is the first date the *next* contract is held; it is ``None`` for the last
        segment, which runs to ``end``. Segments are returned oldest first, which is the order
        history has to be spliced in.
        """
        start, end = _as_date(start), _as_date(end)
        ordered = await self.get_ordered_contracts(root_symbol)
        if not len(ordered):
            return []

        rolls: list[tuple[ExchangeAsset, datetime.date | None]] = []
        current: ExchangeAsset | None = None
        # Walking sessions is exact and cheap enough: a chain covers a few thousand days at most,
        # and both roll rules are pure lookups against pre-loaded data.
        day = start
        while day <= end:
            contract = await self.get_contract_center(root_symbol, day, offset)
            if contract is not None and (current is None or contract.sid != current.sid):
                if current is not None:
                    rolls[-1] = (rolls[-1][0], day)
                rolls.append((contract, None))
                current = contract
            day += datetime.timedelta(days=1)
        return rolls


class CalendarRollFinder(RollFinder):
    """Rolls a fixed offset before the current contract's auto close date.

    The offset is measured in **calendar days** by default. That is rarely what a trader means: a
    three-day offset spanning a weekend is one trading session, not three. Pass a
    ``trading_calendar`` together with ``roll_offset_sessions`` to measure it in sessions instead.

    Args:
        roll_offset_days: Calendar days before auto close to move to the next contract.
        roll_offset_sessions: Trading sessions before auto close to move. Requires
            ``trading_calendar`` and is mutually exclusive with ``roll_offset_days``.
        trading_calendar: Calendar used to count sessions.
    """

    def __init__(self, asset_service, mic: str | None = None, roll_offset_days: int = 0,
                 roll_offset_sessions: int | None = None, trading_calendar=None, logger=None):
        super().__init__(asset_service=asset_service, mic=mic, logger=logger)
        if roll_offset_sessions is not None:
            if roll_offset_days:
                raise ValueError("Pass either roll_offset_days or roll_offset_sessions, not both.")
            if trading_calendar is None:
                raise ValueError("roll_offset_sessions needs a trading_calendar to count sessions.")
        self._roll_offset_days = roll_offset_days
        self._roll_offset_sessions = roll_offset_sessions
        self._trading_calendar = trading_calendar

    def _roll_threshold(self, dt: datetime.date) -> datetime.date:
        """Return the date the front contract is judged against."""
        if self._roll_offset_sessions is None:
            return dt + datetime.timedelta(days=self._roll_offset_days)
        # Counting forward in sessions makes the offset mean the same thing across weekends and
        # holidays, which a calendar-day offset does not.
        sessions = self._trading_calendar.sessions_in_range(
            pd.Timestamp(dt), pd.Timestamp(dt) + pd.Timedelta(days=self._roll_offset_sessions * 7 + 14))
        if len(sessions) > self._roll_offset_sessions:
            return sessions[self._roll_offset_sessions].date()
        return dt + datetime.timedelta(days=self._roll_offset_sessions)

    async def get_contract_center(self, root_symbol: str,
                                  dt: datetime.date | datetime.datetime,
                                  offset: int = 0) -> ExchangeAsset | None:
        dt = _as_date(dt)
        ordered = await self.get_ordered_contracts(root_symbol)
        front = ordered.contract_before_auto_close(self._roll_threshold(dt))
        if front is None:
            return None
        if offset == 0:
            return front
        return ordered.contract_at_offset(front.sid, offset, start_cap=dt)


class VolumeRollFinder(RollFinder):
    """Rolls once the next contract trades more volume than the current one.

    This is how liquidity actually migrates, with the front contract going quiet several
    days before it expires. The roll is forced at ``auto_close_date`` regardless of volume, so a
    contract is never held past its last trading day.

    The comparison uses the **last completed session**, never the one the decision is being made
    in: a session's full volume is not known until it closes, and rolling on it would give the
    backtest information the market had not produced yet.

    Args:
        data_source: Bundle supplying per-``sid`` volume; must expose ``get_dataframe()``.
        grace_period_days: Force the roll this many days before auto close even if the front
            contract still has the larger volume.
    """

    #: A contract needs at least this share of the front's volume before a roll is considered,
    #: which keeps a single illiquid print from triggering a roll.
    THRESHOLD = 1.0

    def __init__(self, asset_service, data_source, mic: str | None = None,
                 grace_period_days: int = 1, logger=None):
        super().__init__(asset_service=asset_service, mic=mic, logger=logger)
        self._data_source = data_source
        self._grace_period_days = grace_period_days
        self._volume_by_sid: dict[int, dict[datetime.date, float]] | None = None
        self._sessions: list[datetime.date] = []

    def _volumes(self) -> dict[int, dict[datetime.date, float]]:
        """Index the bundle's volume column by ``sid`` and session date, once.

        Bar timestamps are stored in UTC, so the session date has to be read in the exchange's
        timezone. Taking ``.dt.date()`` straight off the UTC value shifts every Moscow session back
        by a day, which silently moves every volume-driven roll.
        """
        if self._volume_by_sid is None:
            df = self._data_source.get_dataframe()
            self._volume_by_sid = {}
            if df is not None and not df.is_empty() and "volume" in df.columns:
                calendar = getattr(self._data_source, "trading_calendar", None)
                session = pl.col("date")
                if calendar is not None:
                    session = session.dt.convert_time_zone(str(calendar.tz))
                grouped = df.select(
                    pl.col("sid"),
                    session.dt.date().alias("session"),
                    pl.col("volume"),
                ).group_by(["sid", "session"]).agg(pl.col("volume").sum())
                for sid, session_date, volume in grouped.iter_rows():
                    self._volume_by_sid.setdefault(sid, {})[session_date] = volume
                self._sessions = sorted({day for volumes in self._volume_by_sid.values()
                                         for day in volumes})
        return self._volume_by_sid

    def _last_completed_session(self, day: datetime.date) -> datetime.date | None:
        """Return the most recent session strictly before ``day``.

        Bisecting the session list keeps this cheap; ``None`` means there is no completed session
        to judge by, in which case no volume-driven roll can be justified.
        """
        self._volumes()
        index = bisect.bisect_left(self._sessions, day)
        return self._sessions[index - 1] if index else None

    def _volume(self, sid: int, day: datetime.date) -> float:
        return self._volumes().get(sid, {}).get(day, 0.0)

    async def get_contract_center(self, root_symbol: str,
                                  dt: datetime.date | datetime.datetime,
                                  offset: int = 0) -> ExchangeAsset | None:
        dt = _as_date(dt)
        ordered = await self.get_ordered_contracts(root_symbol)
        if not len(ordered):
            return None

        front = ordered.contract_before_auto_close(
            dt + datetime.timedelta(days=self._grace_period_days))
        if front is None:
            return None

        # Judge liquidity on the last session that has finished, not on the one in progress.
        as_of = self._last_completed_session(dt)
        if as_of is not None:
            # Once the next contract out-trades the front one, liquidity has moved and so do we.
            while True:
                back = ordered.contract_at_offset(front.sid, 1, start_cap=dt)
                if back is None or back.start_date > dt:
                    break
                back_volume = self._volume(back.sid, as_of)
                front_volume = self._volume(front.sid, as_of)
                if back_volume <= 0 or back_volume <= front_volume * self.THRESHOLD:
                    break
                front = back

        if offset == 0:
            return front
        return ordered.contract_at_offset(front.sid, offset, start_cap=dt)


#: Roll style name -> implementation. Used to build ``DataBundle._roll_finders``.
ROLL_FINDERS = {
    "calendar": CalendarRollFinder,
    "volume": VolumeRollFinder,
}
