import datetime
from abc import abstractmethod

import structlog
from exchange_calendars import ExchangeCalendar


class TradingClock:
    trading_calendar: ExchangeCalendar
    emission_rate: datetime.timedelta

    def __init__(self, trading_calendar: ExchangeCalendar, emission_rate: datetime.timedelta):
        self.trading_calendar = trading_calendar
        self.emission_rate = emission_rate
        self._logger = structlog.get_logger(__name__)

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError("__iter__ method must be implemented for trading clock.")
