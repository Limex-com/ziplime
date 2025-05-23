import datetime
from functools import lru_cache

from exchange_calendars import get_calendar, ExchangeCalendar
from pydantic import BaseModel, validator, field_validator


class BaseAlgorithmConfig(BaseModel):
    pass
    # trading_calendar_name: str = "NYSE"
    # exchange: str = "SIMULATION"
    # emission_rate_seconds: int = 60
    #
    # @lru_cache
    # def get_trading_calendar(self) -> ExchangeCalendar:
    #     return get_calendar(self.trading_calendar_name)
    #
    # @lru_cache
    # def get_emission_rate(self) -> datetime.timedelta:
    #     return datetime.timedelta(seconds=self.emission_rate_seconds)
