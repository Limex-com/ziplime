import datetime
from types import SimpleNamespace

import pandas as pd
import polars as pl

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.metrics import default_metrics
from ziplime.finance.metrics_tracker import MetricsTracker
from ziplime.sources.benchmark_source import BenchmarkSource
from ziplime.utils.calendar_utils import get_calendar


def currency(name: str, identity: int) -> Currency:
    return Currency(id=identity, isin=None, asset_name=name, start_date=None, end_date=None,
                    first_traded=None, auto_close_date=None)


def listing(asset, quote, sid: int = 3) -> ExchangeAsset:
    exchange = ExchangeInfo(mic="XNYS", name="XNYS", canonical_name="XNYS", country_code="US")
    return ExchangeAsset(sid=sid, symbol="arbitrary-symbol", start_date=None, end_date=None,
                         first_traded=None, auto_close_date=None, external_id=str(sid),
                         exchange=exchange, asset=asset, quote=quote)


def conversion(stamps, rates) -> CurrencyConversionService:
    return CurrencyConversionService(
        listings=[listing(asset=currency(name="EUR", identity=1), quote=currency(name="USD", identity=2))],
        data=pl.DataFrame(data={"sid": [3] * len(stamps), "date": stamps, "close": rates}),
    )


def equity_listing() -> ExchangeAsset:
    asset = Equity(id=4, isin=None, asset_name="TEST", start_date=None, end_date=None,
                   first_traded=None, auto_close_date=None)
    return listing(asset=asset, quote=currency(name="EUR", identity=1), sid=5)


def reporting_tracker(rates=(1.1, 1.2, 1.3), intraday=False, metrics=None):
    calendar = get_calendar("XNYS")
    sessions = pd.date_range(start="2024-01-02", periods=3, freq="B")
    closes = [calendar.session_close(session=session).to_pydatetime() for session in sessions]
    initial = calendar.session_open(session=sessions[0]).to_pydatetime()
    fx = conversion(stamps=[initial - datetime.timedelta(days=1), *closes], rates=[1.0, *rates])
    reporting = CurrencyReporting(base_currency="EUR", currencies=("EUR", "USD"), conversion=fx)
    ledger = Ledger(trading_sessions=sessions, data_frequency=datetime.timedelta(days=1))
    ledger._portfolio.starting_cash = 100.0
    ledger._portfolio.cash = ledger._portfolio.portfolio_value = 100.0
    ledger.update_account()
    exchange = SimpleNamespace(trading_calendar=calendar, get_start_cash_balance=lambda: 100.0)
    benchmark = BenchmarkSource(
        asset_service=None, trading_calendar=calendar, sessions=pl.Series(values=sessions.date),
        exchange=exchange, emission_rate=datetime.timedelta(days=1),
        benchmark_fields=frozenset({"close"}),
        precalculated_series=pl.DataFrame(data={"date": closes, "close": [0.0] * 3}),
    )
    tracker = MetricsTracker(
        sessions=pl.Series(values=sessions.date), asset_service=None, exchanges={"XNYS": exchange},
        trading_calendar=calendar, first_session=sessions[0], last_session=sessions[-1],
        emission_rate=datetime.timedelta(days=1), ledger=ledger,
        metrics=default_metrics() if metrics is None else metrics,
        benchmark_source=benchmark, intraday_metrics=intraday, currency_reporting=reporting,
    )
    return tracker, ledger, sessions, closes
