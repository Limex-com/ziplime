"""Volume roll finder for roll_finder."""

import bisect
import datetime
import polars as pl
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.services.data_source import DataSource
from ziplime.assets.domain.roll_date_utils import _as_date
from ziplime.assets.domain.roll_finder.roll_finder import RollFinder


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

    def __init__(self, asset_service: AssetService, data_source: DataSource, mic: str | None = None,
                 grace_period_days: int = 1, logger=None) -> None:
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
