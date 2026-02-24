import datetime

import numpy as np
import pandas as pd

from ziplime.assets.entities.asset import Asset
from ziplime.pipeline import Domain
from ziplime.utils.calendar_utils import get_calendar


class AssetCalendarDomain(Domain):
    """An equity domain whose sessions are defined by a named TradingCalendar.

    Parameters
    ----------
    country_code : str
        ISO-3166 two-letter country code of the domain
    calendar_name : str
        Name of the calendar, to be looked by by trading_calendar.get_calendar.
    data_query_offset : np.timedelta64
         The offset from market open when data should no longer be considered
         available for a session. For example, a ``data_query_offset`` of
         ``-np.timedelta64(45, 'm')`` means that the data must have
         been available at least 45 minutes prior to market open for it to
         appear in the pipeline input for the given session.
    """

    def __init__(
            self, assets: list[Asset], calendar_name:str, data_query_offset=-np.timedelta64(45, "m")
    ):
        super().__init__()
        self.calendar_name = calendar_name
        self._data_query_offset = (
            # add one minute because `open_time` is actually the open minute
            # label which is one minute _after_ market open...
                data_query_offset
                - np.timedelta64(1, "m")
        )
        if data_query_offset >= datetime.timedelta(0):
            raise ValueError(
                "data must be ready before market open (offset must be < 0)",
            )
        self._calendar = get_calendar(self.calendar_name)
        self.assets = assets

    @property
    def country_code(self):
        return None

    @property
    def calendar(self):
        return self._calendar

    def sessions(self):
        return self.calendar.sessions

    def data_query_cutoff_for_sessions(self, sessions):
        opens = self.calendar.first_minutes.reindex(sessions)
        missing_mask = pd.isnull(opens)
        if missing_mask.any():
            missing_days = sessions[missing_mask]
            raise ValueError(
                "cannot resolve data query time for sessions that are not on"
                f" the {self.calendar_name} calendar:\n{missing_days}"
            )

        return pd.DatetimeIndex(opens) + self._data_query_offset
