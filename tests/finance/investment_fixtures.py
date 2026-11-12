import datetime
from dataclasses import replace

import pandas as pd
import polars as pl

from tests.finance.currency_fixtures import currency, equity_listing, listing
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.domain.investment_event_kind import InvestmentEventKind
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.monetary_valuation_service import MonetaryValuationService

START = datetime.datetime(year=2024, month=1, day=2, hour=15, tzinfo=datetime.timezone.utc)
END = START + datetime.timedelta(days=1)
SBER = replace(equity_listing(), quote=currency(name="EUR", identity=2), symbol="SBER")
KEY = ("MOEX", "account", SBER)


def accounting_ledger(opening_cash: dict[tuple[str, str, str], float] | None = None) -> Ledger:
    fx = CurrencyConversionService(
        listings=[listing(asset=currency(name="USD", identity=1), quote=currency(name="EUR", identity=2))],
        data=pl.DataFrame(data={"sid": [3, 3], "date": [START, END], "close": [82.0, 200.0]}))
    native = Ledger(pd.DatetimeIndex([START, END]), datetime.timedelta(days=1))
    reporting = CurrencyReporting(base_currency="EUR", currencies=("EUR", "USD"), conversion=fx, investment_pnl=True)
    balances = {("MOEX", "account", "EUR"): 20000} if opening_cash is None else opening_cash
    native.enable_investment_accounting(reporting, START, balances)
    return native


def restore_ledger(valuation: MonetaryValuationService, history) -> Ledger:
    native = Ledger(pd.DatetimeIndex([START, END]), datetime.timedelta(days=1))
    native.native_currency_accounting = True
    native._monetary_valuation = valuation
    native.enable_currency_reporting("EUR", START, valuation.conversion)
    native._investment_base = "EUR"
    native.restore_accounting_history(history)
    return native


def fill(
        identity: str, quantity: float, price: float, dt: datetime.datetime = START, key: InvestmentKey = KEY,
) -> InvestmentEvent:
    return InvestmentEvent(
        id=identity,
        dt=dt,
        kind=InvestmentEventKind.TRADE,
        key=key,
        currency="EUR",
        quantity=quantity,
        unit_value=price,
    )
