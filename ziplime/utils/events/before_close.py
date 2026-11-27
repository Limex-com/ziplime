"""Before close for events."""

import datetime
from ziplime.utils.events.stateless_rule import StatelessRule
from ziplime.utils.events.utils import _build_offset


class BeforeClose(StatelessRule):
    """A rule that triggers for some offset time before the market closes.
    Example that triggers for the last 30 minutes every day:

    >>> BeforeClose(minutes=30)  # doctest: +ELLIPSIS
    <ziplime.utils.events.BeforeClose object at ...>
    """

    def __init__(self, offset: datetime.timedelta | None = None, **kwargs) -> None:
        self.offset = _build_offset(
            offset,
            kwargs,
            datetime.timedelta(minutes=1),  # Defaults to the last minute.
        )

        self._period_start = None
        self._period_close = None
        self._period_end = None

        self._one_minute = datetime.timedelta(minutes=1)

    def calculate_dates(self, dt: datetime.datetime) -> None:
        """
        Given a dt, find that day's close and period start (close - offset).
        """
        period_end = self.cal.session_close(self.cal.minute_to_session(dt))

        # Align the market close time here with the execution time used by the
        # simulation clock. This ensures that scheduled functions trigger at
        # the correct times.
        if self.cal == "us_futures":
            self._period_end = self.cal.execution_time_from_close(period_end)
        else:
            self._period_end = period_end

        self._period_start = self._period_end - self.offset
        self._period_close = self._period_end

    def should_trigger(self, dt: datetime.datetime):
        # There are two reasons why we might want to recalculate the dates.
        # One is the first time we ever call should_trigger, when
        # self._period_start is none. The second is when we're on a new day,
        # and need to recalculate the dates. For performance reasons, we rely
        # on the fact that our clock only ever ticks forward, since it's
        # cheaper to do dt1 <= dt2 than dt1.date() != dt2.date(). This means
        # that we will NOT correctly recognize a new date if we go backwards
        # in time(which should never happen in a simulation, or in live
        # trading)
        if self._period_start is None or self._period_close <= dt:
            self.calculate_dates(dt)

        # The first bar at or after the target -- see `AfterOpen.should_trigger`. One consequence
        # is worth knowing: an offset smaller than the bar length can leave no bar to fire on at
        # all. `market_close(minutes=1)` on a five-minute grid asks for 15:59 and the session's
        # last bar is 15:56, so it does not fire that day. Ask for an offset at least as long as
        # a bar.
        return dt >= self._period_start
