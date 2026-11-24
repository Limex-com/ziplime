"""Time rules for events."""

import datetime
from ziplime.utils.events.after_open import AfterOpen
from ziplime.utils.events.always import Always
from ziplime.utils.events.before_close import BeforeClose


class time_rules:
    """Factories for time-based :func:`~ziplime.api.schedule_function` rules.

    See Also
    --------
    :func:`~ziplime.api.schedule_function`
    """

    @staticmethod
    def market_open(
        offset: datetime.timedelta | None = None,
        hours: int | None = None,
        minutes: int | None = None,
    ) -> AfterOpen:
        """
        Create a rule that triggers at a fixed offset from market open.

        The offset can be specified either as a :class:`datetime.timedelta`, or
        as a number of hours and minutes.

        Parameters
        ----------
        offset : datetime.timedelta, optional
            If passed, the offset from market open at which to trigger. Must be
            at least 1 minute.
        hours : int, optional
            If passed, number of hours to wait after market open.
        minutes : int, optional
            If passed, number of minutes to wait after market open.

        Returns
        -------
        rule : ziplime.utils.events.EventRule

        Notes
        -----
        If no arguments are passed, the default offset is one minute after
        market open.

        If ``offset`` is passed, ``hours`` and ``minutes`` must not be
        passed. Conversely, if either ``hours`` or ``minutes`` are passed,
        ``offset`` must not be passed.
        """
        return AfterOpen(offset=offset, hours=hours, minutes=minutes)

    @staticmethod
    def market_close(
        offset: datetime.timedelta | None = None,
        hours: int | None = None,
        minutes: int | None = None,
    ) -> BeforeClose:
        """
        Create a rule that triggers at a fixed offset from market close.

        The offset can be specified either as a :class:`datetime.timedelta`, or
        as a number of hours and minutes.

        Parameters
        ----------
        offset : datetime.timedelta, optional
            If passed, the offset from market close at which to trigger. Must
            be at least 1 minute.
        hours : int, optional
            If passed, number of hours to wait before market close.
        minutes : int, optional
            If passed, number of minutes to wait before market close.

        Returns
        -------
        rule : ziplime.utils.events.EventRule

        Notes
        -----
        If no arguments are passed, the default offset is one minute before
        market close.

        If ``offset`` is passed, ``hours`` and ``minutes`` must not be
        passed. Conversely, if either ``hours`` or ``minutes`` are passed,
        ``offset`` must not be passed.
        """
        return BeforeClose(offset=offset, hours=hours, minutes=minutes)

    every_minute = Always
