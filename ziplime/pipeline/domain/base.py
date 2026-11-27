"""Base for domain."""

import pandas as pd


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

    def roll_forward(self, dt: pd.Timestamp):
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
