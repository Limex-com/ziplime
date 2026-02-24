import datetime

import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.trading.enums.simulation_event import SimulationEvent
from ziplime.gens.domain.trading_clock import TradingClock


class SimulationClock(TradingClock):
    def __init__(self,
                 start_date: datetime.datetime,
                 end_date: datetime.datetime,
                 trading_calendar: ExchangeCalendar,
                 emission_rate: datetime.timedelta):
        super().__init__(trading_calendar=trading_calendar, emission_rate=emission_rate)
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

        self.start_session = start_date.date()
        self.end_session = end_date.date()

        if trading_calendar.sessions_distance(self.start_session, self.end_session) < 1:
            raise Exception(
                f"There are no trading days between {self.start_session} and {self.end_session}")
        if not trading_calendar.is_session(self.start_session):
            # if the start date is not a valid session in this calendar,
            # push it forward to the first valid session
            self.start_session = trading_calendar.minute_to_session(
                self.start_session
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
        self.minutes_by_session = self.calc_minutes_by_session()

    def calc_minutes_by_session(self):
        minutes_by_session_n = {}
        if self.emission_rate < datetime.timedelta(days=1):
            for session_idx, session in enumerate(self.sessions):
                minutes = pl.datetime_range(self.market_opens[session_idx], self.market_closes[session_idx],
                                            interval=self.emission_rate,
                                            eager=True)
                minutes_by_session_n[session] = minutes
        else:
            minutes_by_session_n = {session: pl.Series([self.market_closes[session_idx]]) for
                                    session_idx, session in enumerate(self.sessions)}
        return minutes_by_session_n

    def __iter__(self):
        for idx, session in enumerate(self.sessions):
            yield session, SimulationEvent.SESSION_START

            bts_minute = self.before_trading_start_minutes[idx]
            regular_minutes = self.minutes_by_session[session]

            yield bts_minute, SimulationEvent.BEFORE_TRADING_START_BAR
            for minute in regular_minutes:
                yield minute, SimulationEvent.BAR
                yield minute, SimulationEvent.EMISSION_RATE_END

            yield regular_minutes[-1], SimulationEvent.SESSION_END
