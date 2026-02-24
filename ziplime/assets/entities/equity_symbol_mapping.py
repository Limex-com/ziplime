import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class EquitySymbolMapping:
    symbol: str
    company_symbol: str
    share_class_symbol: str
    start_date: datetime.date
    end_date: datetime.date
    exchange_name: str
