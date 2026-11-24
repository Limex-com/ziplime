"""Equity session domain for domain."""

import datetime
import pandas as pd
import pytz
from exchange_calendars.utils.pandas_utils import days_at_time
from ziplime.pipeline.domain.base import Domain


class EquitySessionDomain(Domain):
    """A domain built directly from an index of sessions.

    Mostly useful for testing.

    Parameters
    ----------
    sessions : pd.DatetimeIndex
        Sessions to use as output labels for pipelines run on this domain.
    country_code : str
        ISO 3166 country code of equities to be used with this domain.
    data_query_time : datetime.time, optional
        The time of day when data should no longer be considered available for
        a session.
    data_query_date_offset : int, optional
        The number of days to add to the session label before applying the
        ``data_query_time``. This can be used to express that the cutoff time
        for a session falls on a different calendar day from the session label.
    """

    def __init__(
            self,
            sessions: pd.DatetimeIndex,
            country_code: str,
            data_query_time: datetime.time | None = None,
            data_query_date_offset: int = 0,
    ) -> None:
        self._country_code = country_code
        self._sessions = sessions

        if data_query_time is None:
            data_query_time = datetime.time(0, 0, tzinfo=pytz.timezone("UTC"))

        if data_query_time.tzinfo is None:
            raise ValueError("data_query_time cannot be tz-naive")

        self._data_query_time = data_query_time
        self._data_query_date_offset = data_query_date_offset

    @property
    def country_code(self) -> str:
        return self._country_code

    def sessions(self) -> pd.DatetimeIndex:
        return self._sessions

    def data_query_cutoff_for_sessions(self, sessions: pd.DatetimeIndex) -> pd.DatetimeIndex:
        return days_at_time(
            sessions,
            self._data_query_time,
            self._data_query_time.tzinfo,
            self._data_query_date_offset,
        )
