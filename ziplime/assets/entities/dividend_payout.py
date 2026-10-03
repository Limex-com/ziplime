import datetime
from dataclasses import dataclass

from iso4217 import Currency
from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class DividendPayout:
    asset: Asset
    amount: float
    pay_date: datetime.date
    declared_date: datetime.date
    record_date: datetime.date
    ex_date: datetime.date
    currency: Currency
