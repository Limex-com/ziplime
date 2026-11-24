"""Date rules for events."""

from ziplime.utils.events.always import Always
from ziplime.utils.events.n_days_before_last_trading_day_of_month import NDaysBeforeLastTradingDayOfMonth
from ziplime.utils.events.n_days_before_last_trading_day_of_quarter import NDaysBeforeLastTradingDayOfQuarter
from ziplime.utils.events.n_days_before_last_trading_day_of_week import NDaysBeforeLastTradingDayOfWeek
from ziplime.utils.events.nth_trading_day_of_month import NthTradingDayOfMonth
from ziplime.utils.events.nth_trading_day_of_quarter import NthTradingDayOfQuarter
from ziplime.utils.events.nth_trading_day_of_week import NthTradingDayOfWeek


class date_rules:
    """Factories for date-based :func:`~ziplime.api.schedule_function` rules.

    See Also
    --------
    :func:`~ziplime.api.schedule_function`
    """

    @staticmethod
    def every_day() -> Always:
        """Create a rule that triggers every day.

        Returns
        -------
        rule : ziplime.utils.events.EventRule
        """
        return Always()

    @staticmethod
    def month_start(days_offset: int = 0) -> NthTradingDayOfMonth:
        """
        Create a rule that triggers a fixed number of trading days after the
        start of each month.

        Parameters
        ----------
        days_offset : int, optional
            Number of trading days to wait before triggering each
            month. Default is 0, i.e., trigger on the first trading day of the
            month.

        Returns
        -------
        rule : ziplime.utils.events.EventRule
        """
        return NthTradingDayOfMonth(n=days_offset)

    @staticmethod
    def month_end(days_offset: int = 0) -> NDaysBeforeLastTradingDayOfMonth:
        """
        Create a rule that triggers a fixed number of trading days before the
        end of each month.

        Parameters
        ----------
        days_offset : int, optional
            Number of trading days prior to month end to trigger. Default is 0,
            i.e., trigger on the last day of the month.

        Returns
        -------
        rule : ziplime.utils.events.EventRule
        """
        return NDaysBeforeLastTradingDayOfMonth(n=days_offset)

    @staticmethod
    def quarter_start(days_offset: int = 0) -> NthTradingDayOfQuarter:
        """Create a rule that triggers a fixed number of trading days after each quarter starts.

        Parameters
        ----------
        days_offset : int, optional
            Trading days to wait before triggering each quarter. Default is 0, i.e. trigger on the
            first trading day of January, April, July and October.

        Returns
        -------
        rule : ziplime.utils.events.EventRule
        """
        return NthTradingDayOfQuarter(n=days_offset)

    @staticmethod
    def quarter_end(days_offset: int = 0) -> NDaysBeforeLastTradingDayOfQuarter:
        """Create a rule that triggers a fixed number of trading days before each quarter ends.

        Parameters
        ----------
        days_offset : int, optional
            Trading days prior to quarter end to trigger. Default is 0, i.e. trigger on the last
            trading day of the quarter.

        Returns
        -------
        rule : ziplime.utils.events.EventRule
        """
        return NDaysBeforeLastTradingDayOfQuarter(n=days_offset)

    @staticmethod
    def week_start(days_offset: int = 0) -> NthTradingDayOfWeek:
        """
        Create a rule that triggers a fixed number of trading days after the
        start of each week.

        Parameters
        ----------
        days_offset : int, optional
            Number of trading days to wait before triggering each week. Default
            is 0, i.e., trigger on the first trading day of the week.
        """
        return NthTradingDayOfWeek(n=days_offset)

    @staticmethod
    def week_end(days_offset: int = 0) -> NDaysBeforeLastTradingDayOfWeek:
        """
        Create a rule that triggers a fixed number of trading days before the
        end of each week.

        Parameters
        ----------
        days_offset : int, optional
            Number of trading days prior to week end to trigger. Default is 0,
            i.e., trigger on the last trading day of the week.
        """
        return NDaysBeforeLastTradingDayOfWeek(n=days_offset)
