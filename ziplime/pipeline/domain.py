"""
This module defines the interface and implementations of Pipeline domains.

A domain represents a set of labels for the arrays computed by a Pipeline.
Currently, this means that a domain defines two things:

1. A calendar defining the dates to which the pipeline's inputs and outputs
   should be aligned. The calendar is represented concretely by a pandas
   DatetimeIndex.

2. The set of assets that the pipeline should compute over. Right now, the only
   supported way of representing this set is with a two-character country code
   describing the country of assets over which the pipeline should compute. In
   the future, we expect to expand this functionality to include more general
   concepts.
"""

import datetime
from textwrap import dedent
from collections.abc import Callable

import numpy as np
import pandas as pd
import pytz

from ziplime.errors import NoFurtherDataError  # noqa: F401 - retained for compatibility with public imports
from ziplime.utils.calendar_utils import get_calendar

from ziplime.utils.formatting import bulleted_list
from exchange_calendars.utils.pandas_utils import days_at_time


class Domain:
    """A domain represents a set of labels for the arrays computed by a Pipeline.

    A domain defines two things:

    1. A calendar defining the dates to which the pipeline's inputs and outputs
       should be aligned. The calendar is represented concretely by a pandas
       DatetimeIndex.

    2. The set of assets that the pipeline should compute over. Right now, the only
       supported way of representing this set is with a two-character country code
       describing the country of assets over which the pipeline should compute. In
       the future, we expect to expand this functionality to include more general
       concepts.
    """

    def __init__(self) -> None:
        self.assets = None

    def sessions(self) -> pd.DatetimeIndex:
        """Get all trading sessions for the calendar of this domain.

        This determines the row labels of Pipeline outputs for pipelines run on
        this domain.

        Returns
        -------
        sessions : pd.DatetimeIndex
            An array of all session labels for this domain.
        """

    @property
    def country_code(self) -> str:
        """The country code for this domain.

        Returns
        -------
        code : str
            The two-character country iso3166 country code for this domain.
        """

    def data_query_cutoff_for_sessions(self, sessions: pd.DatetimeIndex) -> pd.DatetimeIndex:
        """Compute the data query cutoff time for the given sessions.

        Parameters
        ----------
        sessions : pd.DatetimeIndex
            The sessions to get the data query cutoff times for. This index
            will contain all midnight UTC values.

        Returns
        -------
        data_query_cutoff : pd.DatetimeIndex
            Timestamp of the last minute for which data should be considered
            "available" on each session.
        """

    def roll_forward(self, dt: 'pd.Timestamp'):
        """Given a date, align it to the calendar of the pipeline's domain.

        Parameters
        ----------
        dt : pd.Timestamp

        Returns
        -------
        pd.Timestamp
        """
        dt = pd.Timestamp(dt)
        trading_days = self.sessions()
        try:
            return trading_days[trading_days.searchsorted(dt)]
        except IndexError as exc:
            raise ValueError(
                f"Date {dt.date()} was past the last session for domain {self}. "
                f"The last session for this domain is {trading_days[-1].date()}."
            ) from exc


class GenericDomain(Domain):
    """Special singleton class used to represent generic DataSets and Columns."""

    def sessions(self) -> pd.DatetimeIndex:
        raise NotImplementedError("Can't get sessions for generic domain.")

    @property
    def country_code(self) -> str:
        raise NotImplementedError("Can't get country code for generic domain.")

    def data_query_cutoff_for_sessions(self, sessions: pd.DatetimeIndex) -> pd.DatetimeIndex:
        raise NotImplementedError(
            "Can't compute data query cutoff times for generic domain.",
        )

    def __repr__(self) -> str:
        return "GENERIC"


GENERIC = GenericDomain()


class EquityCalendarDomain(Domain):
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
            self, country_code: str, calendar_name: str, data_query_offset: 'np.timedelta64' = -np.timedelta64(45, "m")
    ) -> None:
        self._country_code = country_code
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

    @property
    def country_code(self) -> str:
        return self._country_code

    # @lazyval
    @property
    def calendar(self):
        return get_calendar(self.calendar_name)

    def sessions(self) -> pd.DatetimeIndex:
        return self.calendar.sessions

    def data_query_cutoff_for_sessions(self, sessions: pd.DatetimeIndex) -> pd.DatetimeIndex:
        opens = self.calendar.first_minutes.reindex(sessions)
        missing_mask = pd.isnull(opens)
        if missing_mask.any():
            missing_days = sessions[missing_mask]
            raise ValueError(
                "cannot resolve data query time for sessions that are not on"
                f" the {self.calendar_name} calendar:\n{missing_days}"
            )

        return pd.DatetimeIndex(opens) + self._data_query_offset

    def __repr__(self) -> str:
        return "EquityCalendarDomain({!r}, {!r})".format(
            self.country_code,
            self.calendar_name,
        )


# This would be better if we provided more context for which domains came from
# which terms.
class AmbiguousDomain(Exception):
    """Raised when we attempt to infer a domain from a collection of mixed terms."""

    _TEMPLATE = dedent(
        """\
        Found terms with conflicting domains:
        {domains}"""
    )

    def __init__(self, domains: 'list[Domain]') -> None:
        self.domains = domains

    def __str__(self):
        return self._TEMPLATE.format(
            domains=bulleted_list(self.domains, indent=2),
        )


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


def __getattr__(name: str) -> Callable:
    # Preserve the public import without making domain definitions depend on Term.
    if name == "infer_domain":
        from ziplime.pipeline.domain_inference import infer_domain

        globals()[name] = infer_domain
        return infer_domain
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"infer_domain"})
