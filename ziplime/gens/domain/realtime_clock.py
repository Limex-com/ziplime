import datetime
import time

import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.gens.domain.trading_clock import TradingClock


class RealtimeClock(TradingClock):
    def __init__(self,
                 trading_calendar: ExchangeCalendar,
                 emission_rate: datetime.timedelta,
                 start_date: datetime.datetime | None = None,
                 end_date: datetime.datetime | None = None,
                 timedelta_diff_from_current_time: datetime.timedelta = None,
                 ):
        super().__init__(trading_calendar=trading_calendar, emission_rate=emission_rate)
        if start_date is not None and end_date is not None:
            if start_date >= end_date:
                raise ValueError("Period start falls after period end.")
            if start_date >= trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
                raise ValueError("Period start falls after the last known trading day.")
            if end_date <= trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
                raise ValueError("Period end falls before the first known trading day.")

            # chop off any minutes or hours on the given start and end dates,
            # as we only support session labels here (and we represent session
            # labels as midnight UTC).

        self.start_date = start_date
        self.end_date = end_date

        if self.start_date is None:
            self.start_date = datetime.datetime.now(tz=trading_calendar.tz)

        self.start_session = self.start_date.date()

        if self.end_date is None:
            self.end_date = datetime.datetime.now(tz=trading_calendar.tz) + datetime.timedelta(days=10)
        self.end_session = self.end_date.date()

        if trading_calendar.sessions_distance(self.start_session, self.end_session) < 1:
            raise Exception(
                f"There are no trading days between {self.start_session} and {self.end_session}")

        if not trading_calendar.is_session(self.start_session):
            # if the start date is not a valid session in this calendar,
            # push it forward to the first valid session
            self.start_session = trading_calendar.minute_to_session(
                self.start_session, direction="next"
            ).tz_localize(self.trading_calendar.tz).to_pydatetime().date()

        if not trading_calendar.is_session(self.end_session):
            # if the end date is not a valid session in this calendar,
            # pull it backward to the last valid session before the given
            # end date.
            self.end_session = trading_calendar.minute_to_session(
                self.end_session, direction="previous"
            ).tz_localize(self.trading_calendar.tz).to_pydatetime().date()

        self.first_open = trading_calendar.session_first_minute(
            self.start_session
        ).tz_convert(self.trading_calendar.tz).to_pydatetime()
        self.last_close = trading_calendar.session_close(
            self.end_session
        ).tz_convert(self.trading_calendar.tz).to_pydatetime()

        self.sessions = pl.Series(self.trading_calendar.sessions_in_range(
            self.start_session, self.end_session)
        ).dt.date()

        self.market_closes = pl.Series(
            self.trading_calendar.schedule.loc[self.sessions, "close"].dt.tz_convert(
                self.trading_calendar.tz))
        self.market_opens = pl.Series(
            self.trading_calendar.first_minutes.loc[self.sessions].dt.tz_convert(
                self.trading_calendar.tz))

        self.before_trading_start_minutes = self.market_opens - datetime.timedelta(minutes=46)

        if timedelta_diff_from_current_time is None:
            self.timedelta_diff_from_current_time = datetime.timedelta(seconds=0)
        else:
            self.timedelta_diff_from_current_time = timedelta_diff_from_current_time
        self.before_trading_start_minutes = self.before_trading_start_minutes
        self.before_trading_start_bar_yielded = len(self.before_trading_start_minutes) * [False]
        self.market_closes_yielded = len(self.market_closes) * [False]
        self.market_opens_yielded = len(self.market_opens) * [False]

    def _sleep_and_increase_time(self, sleep_seconds: int) -> datetime.datetime:
        time.sleep(sleep_seconds)
        now = datetime.datetime.now(tz=self.trading_calendar.tz) + self.timedelta_diff_from_current_time
        return now

    def __iter__(self):
        current_time = datetime.datetime.now(tz=self.trading_calendar.tz) + self.timedelta_diff_from_current_time
        last_bar_emit: datetime.datetime | None = None
        while current_time <= self.market_closes[-1]:
            current_session_index = self.sessions.index_of(current_time.date())
            if current_session_index is None:
                # happens if current time is not in trading hours, so we need to wait till market hours
                self._logger.info(f"Current time {current_time} is not in trading hours, waiting till market hours.")
                current_time = self._sleep_and_increase_time(sleep_seconds=1)
                continue
            if current_time >= self.before_trading_start_minutes[current_session_index] and not \
                    self.before_trading_start_bar_yielded[current_session_index]:
                self.before_trading_start_bar_yielded[current_session_index] = True
                yield current_time, SimulationEvent.BEFORE_TRADING_START_BAR
            elif current_time < self.market_opens[current_session_index]:
                current_time = self._sleep_and_increase_time(sleep_seconds=1)
            elif self.market_opens[current_session_index] <= current_time <= self.market_closes[
                current_session_index] and not self.market_opens_yielded[current_session_index]:
                self.market_opens_yielded[current_session_index] = True
                yield current_time.date(), SimulationEvent.SESSION_START
            elif self.market_opens[current_session_index] <= current_time <= self.market_closes[current_session_index]:
                if last_bar_emit is None or current_time - last_bar_emit >= self.emission_rate:
                    last_bar_emit = current_time
                    yield current_time, SimulationEvent.BAR
                    current_time = datetime.datetime.now(
                        tz=self.trading_calendar.tz) + self.timedelta_diff_from_current_time
                    yield current_time, SimulationEvent.EMISSION_RATE_END
                else:
                    current_time = self._sleep_and_increase_time(sleep_seconds=1)
            elif current_time > self.market_closes[current_session_index]:
                if self.market_closes_yielded[current_session_index]:
                    self._logger.info(
                        f"Current time {current_time} is after trading hours, waiting till market hours.")
                    current_time = self._sleep_and_increase_time(sleep_seconds=1)
                else:
                    last_bar_emit = current_time
                    self.market_closes_yielded[current_session_index] = True
                    yield current_time.date(), SimulationEvent.SESSION_END
            else:
                # We should never end up in this branch
                raise RuntimeError("Invalid state in RealtimeClock")
