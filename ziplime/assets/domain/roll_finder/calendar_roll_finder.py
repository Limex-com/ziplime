"""Calendar roll finder for roll_finder."""

import datetime
import pandas as pd
from exchange_calendars import ExchangeCalendar
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.assets.domain.roll_date_utils import _as_date
from ziplime.assets.domain.roll_finder.roll_finder import RollFinder


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

    def __init__(
        self,
        asset_service: AssetService,
        mic: str | None = None,
        roll_offset_days: int = 0,
        roll_offset_sessions: int | None = None,
        trading_calendar: ExchangeCalendar | None = None,
        logger=None,
    ) -> None:
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
