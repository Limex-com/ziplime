import datetime
from exchange_calendars import ExchangeCalendar
import polars as pl


class SimulationParameters:
    def __init__(
            self,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
            trading_calendar: ExchangeCalendar,
            emission_rate: datetime.timedelta,
    ):

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

        self.emission_rate = emission_rate

        self.trading_calendar = trading_calendar
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
