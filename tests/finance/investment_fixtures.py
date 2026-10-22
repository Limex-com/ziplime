import datetime
from dataclasses import replace

import polars as pl

from tests.finance.currency_fixtures import currency, equity_listing, listing
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.investment_book import InvestmentBook
from ziplime.finance.monetary_valuation_service import MonetaryValuationService

START = datetime.datetime(year=2024, month=1, day=2, hour=15, tzinfo=datetime.timezone.utc)
END = START + datetime.timedelta(days=1)
SBER = replace(equity_listing(), quote=currency(name="RUB", identity=2), symbol="SBER")
KEY = ("MOEX", "account", SBER)


def book() -> InvestmentBook:
    fx = CurrencyConversionService(
        listings=[listing(asset=currency(name="USD", identity=1), quote=currency(name="RUB", identity=2))],
        data=pl.DataFrame(data={"sid": [3, 3], "date": [START, END], "close": [82.0, 200.0]}))
    return InvestmentBook(valuation=MonetaryValuationService(conversion=fx))


def fill(
        identity: str, quantity: float, price: float, dt: datetime.datetime = START, key: InvestmentKey = KEY,
) -> InvestmentEvent:
    return InvestmentEvent(
        id=identity,
        dt=dt,
        kind="trade",
        key=key,
        currency="RUB",
        quantity=quantity,
        unit_value=price,
    )
