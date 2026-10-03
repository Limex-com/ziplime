import datetime
from dataclasses import dataclass

from iso4217 import Currency
from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class StockDividendPayout:
    asset: Asset
    amount: float
    pay_date: datetime.date
    currency: Currency
