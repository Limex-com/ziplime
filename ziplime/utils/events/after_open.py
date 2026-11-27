"""After open for events."""

import datetime
from ziplime.utils.events.stateless_rule import StatelessRule
from ziplime.utils.events.utils import _build_offset


class AfterOpen(StatelessRule):
    """
    A rule that triggers for some offset after the market opens.
    Example that triggers after 30 minutes of the market opening:

    >>> AfterOpen(minutes=30)  # doctest: +ELLIPSIS
    <ziplime.utils.events.AfterOpen object at ...>
    """

    def __init__(self, offset: datetime.timedelta | None = None, **kwargs) -> None:
        self.offset = _build_offset(
            offset,
            kwargs,
            datetime.timedelta(minutes=1),  # Defaults to the first minute.
        )

        self._period_start = None
        self._period_end = None
        self._period_close = None

        self._one_minute = datetime.timedelta(minutes=1)

    def calculate_dates(self, dt: datetime.datetime) -> None:
        """Given a date, find that day's open and period end (open + offset)."""

        period_start = self.cal.session_first_minute(self.cal.minute_to_session(dt))
        period_close = self.cal.session_close(self.cal.minute_to_session(dt))

        # Align the market open and close times here with the execution times
        # used by the simulation clock. This ensures that scheduled functions
        # trigger at the correct times.
        if self.cal.name == "us_futures":
            self._period_start = self.cal.execution_time_from_open(period_start)
            self._period_close = self.cal.execution_time_from_close(period_close)
        else:
            self._period_start = period_start
            self._period_close = period_close

        self._period_end = self._period_start + self.offset - self._one_minute

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

        # The first bar at or after the target, not the bar *equal* to it. `OncePerDay` wraps this
        # rule, so only that first one gets through. Equality assumes a bar exists on every minute,
        # which is true only when the emission rate is one minute: on a five-minute grid the bars
        # fall at 09:31, 09:36, 09:41 and `market_open(minutes=30)` asks for 10:00, which is not
        # one of them, so the function was never called and nothing said so.
        return dt >= self._period_end
