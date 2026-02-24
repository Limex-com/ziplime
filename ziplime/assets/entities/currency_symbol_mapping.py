import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class CurrencySymbolMapping:
    symbol: str
    start_date: datetime.date
    end_date: datetime.date
    exchange_name: str
