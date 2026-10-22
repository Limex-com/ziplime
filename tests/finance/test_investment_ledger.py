import datetime
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from iso4217 import Currency

from tests.finance.investment_fixtures import book, SBER, KEY, START, END
from tests.finance.currency_fixtures import currency
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.split import Split
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.reporting_ledger import ReportingLedger
from ziplime.finance.domain.transaction import Transaction


def ledger():
    native = Ledger(trading_sessions=pd.DatetimeIndex(data=[START, END]), data_frequency=datetime.timedelta(days=1))
    reporting = CurrencyReporting(
        base_currency="RUB", currencies=("RUB", "USD"), conversion=book().valuation.conversion, investment_pnl=True,
        opening_cash={("MOEX", "account", "RUB"): 10000, ("MOEX", "account", "USD"): 150})
    native.enable_currency_reporting(base_currency="RUB", dt=START)
    native.enable_investment_accounting(reporting=reporting, dt=START, opening_cash=dict(reporting.opening_cash))
    return native, reporting


def transaction(identity, amount, price, dt=START, asset=SBER):
    return Transaction(
        id=identity,
        amount=amount,
        dt=dt,
        price=price,
        exchange_name="MOEX",
        trading_account_id="account",
        asset=asset,
    )


@pytest.mark.asyncio
async def test_mixed_native_cash_positions_and_reporting_reconcile():
    native, reporting = ledger()
    usd = ReportingLedger(source=native, reporting=reporting, currency="USD", initial_dt=START)
    us = replace(SBER, sid=55, quote=currency(name="USD", identity=1))
    native.process_transaction(transaction=transaction(identity="sber", amount=100, price=100))
    native.process_transaction(transaction=transaction(identity="us", amount=5, price=30, asset=us))
    native.set_reporting_datetime(dt=END)
    for asset, price in ((SBER, 110), (us, 30)):
        native.position_tracker.update_position(
            asset=asset, exchange_name="MOEX", trading_account_id="account", last_sale_price=price, last_sale_date=END)
    await native.update_portfolio()
    usd.refresh(dt=END)
    assert native.investments.cash_balances(dt=END) == {
        ("MOEX", "account", "RUB"): 0, ("MOEX", "account", "USD"): 0}
    assert native.portfolio.portfolio_value == pytest.approx(expected=41000)
    assert native.position_tracker.stats.net_value == pytest.approx(expected=41000)
    assert usd.portfolio.portfolio_value == 205
    assert usd.portfolio.pnl == pytest.approx(expected=55 - 10000 / 82)
    assert usd.investment.total_pnl == pytest.approx(expected=usd.portfolio.pnl)
    assert usd.investment.historical_cost == pytest.approx(expected=10000 / 82 + 150)


@pytest.mark.asyncio
async def test_commissions_are_fill_linked_and_idempotent():
    native, _ = ledger()
    native.process_transaction(transaction=transaction(identity="sber", amount=100, price=100))
    native.process_transaction(transaction=transaction(identity="sber", amount=100, price=100))
    commission = Commission(asset=SBER, order=None, amount=82, transaction_id="sber", dt=START, id="fee")
    native.process_commission(commission=commission, tr=None)
    native.process_commission(commission=commission, tr=None)
    await native.update_portfolio()
    assert native.investment_snapshot(currency="USD", dt=START).fees == 1
    assert native.investment_snapshot(currency="USD", dt=START).unrealized_pnl == pytest.approx(expected=-1)
    assert native.investments.cash_balances(dt=START)[("MOEX", "account", "RUB")] == -82
    with pytest.raises(expected_exception=ValueError, match="transaction_id"):
        native.process_commission(commission=Commission(asset=SBER, order=None, amount=82), tr=None)


@pytest.mark.asyncio
async def test_currency_labelled_external_funding_is_not_pnl():
    native, reporting = ledger()
    usd = ReportingLedger(source=native, reporting=reporting, currency="USD", initial_dt=START)
    native.set_reporting_datetime(dt=END)
    await native.capital_change(
        change_amount=100, currency="USD", exchange_name="MOEX", trading_account_id="account", event_id="deposit")
    await native.capital_change(
        change_amount=100, currency="USD", exchange_name="MOEX", trading_account_id="account", event_id="deposit")
    await native.update_portfolio()
    usd.refresh(dt=END)
    assert native.investments.cash_balances(dt=END)[("MOEX", "account", "USD")] == 250
    assert usd.portfolio.pnl == pytest.approx(expected=10000 / 200 - 10000 / 82)
    assert usd.investment.total_pnl == 0


@pytest.mark.asyncio
async def test_disposal_realizes_security_pnl_and_retained_cash_keeps_its_fx_exposure():
    native, reporting = ledger()
    usd = ReportingLedger(source=native, reporting=reporting, currency="USD", initial_dt=START)
    native.process_transaction(transaction=transaction(identity="buy", amount=100, price=100))
    native.process_transaction(transaction=transaction(identity="sell", amount=-100, price=110))
    await native.update_portfolio()
    native.set_reporting_datetime(dt=END)
    await native.update_portfolio()
    usd.refresh(dt=END)
    assert usd.investment.realized_pnl == pytest.approx(expected=1000 / 82)
    assert usd.portfolio.portfolio_value == 11000 / 200 + 150
    assert usd.portfolio.pnl == pytest.approx(expected=11000 / 200 - 10000 / 82)


def test_opening_positions_require_explicit_history():
    native, reporting = ledger()
    native.process_transaction(transaction=transaction(identity="buy", amount=100, price=100))
    restored = Ledger(trading_sessions=pd.DatetimeIndex(data=[START, END]), data_frequency=datetime.timedelta(days=1))
    restored.position_tracker.update_position(
        asset=SBER, exchange_name="MOEX", trading_account_id="account", amount=100, cost_basis=100,
        last_sale_price=100, last_sale_date=START)
    restored.enable_currency_reporting(base_currency="RUB", dt=START)
    with pytest.raises(expected_exception=ValueError, match="historical"):
        restored.enable_investment_accounting(reporting=reporting, dt=START, opening_cash=dict(reporting.opening_cash))
    config = replace(reporting, investment_history=native.investments.history())
    restored.enable_investment_accounting(reporting=config, dt=START, opening_cash=dict(config.opening_cash))
    assert restored.investment_snapshot(
        currency="USD",
        dt=START,
    ) == native.investment_snapshot(currency="USD", dt=START)


@pytest.mark.asyncio
async def test_dividend_entitlements_keep_account_scope_payment_currency_and_payment_time_fx_after_sale():
    native, reporting = ledger()
    native.process_transaction(transaction=transaction(identity="long", amount=100, price=100))
    native.process_transaction(
        transaction=replace(transaction(identity="short", amount=-40, price=100), trading_account_id="short-account"),
    )
    dividend = DividendPayout(
        asset=SBER.asset,
        amount=1.0,
        pay_date=END.date(),
        declared_date=START.date(),
        record_date=START.date(),
        ex_date=START.date(),
        currency=Currency.USD,
    )
    service = SimpleNamespace(get_cash_dividends_with_ex_date=AsyncMock(return_value=[dividend]))
    await native.process_dividends(next_session=START.date(), asset_service=service)
    await native.process_dividends(next_session=START.date(), asset_service=service)
    native.process_transaction(transaction=transaction(identity="sell", amount=-100, price=110))
    native.process_transaction(
        transaction=replace(transaction(identity="cover", amount=40, price=110), trading_account_id="short-account"),
    )
    restored = Ledger(trading_sessions=pd.DatetimeIndex(data=[START, END]), data_frequency=datetime.timedelta(days=1))
    restored.enable_investment_accounting(
        reporting=replace(reporting, investment_history=native.investment_history()), dt=START, opening_cash={})
    native.set_reporting_datetime(dt=END)
    await native.process_dividends(next_session=END.date(), asset_service=service)
    await native.process_dividends(next_session=END.date(), asset_service=service)
    result = native.investment_snapshot(currency="USD", dt=END)
    assert result.income == 60
    assert [position.income for position in result.positions] == [100, -40]
    assert native.investment_snapshot(currency="RUB", dt=END).income == 60 * 200
    assert native.investments.cash_balances(dt=END)[("MOEX", "short-account", "USD")] == -40
    assert native.investments.cash_balances(dt=END)[("MOEX", "account", "USD")] == 250
    restored.set_reporting_datetime(dt=END)
    await restored.process_dividends(next_session=END.date(), asset_service=service)
    assert restored.investment_snapshot(currency="USD", dt=END) == result
    assert restored.investments.cash_balances(dt=END) == native.investments.cash_balances(dt=END)


def test_splits_are_idempotent_and_reject_the_whole_batch_before_mutating_positions():
    native, _ = ledger()
    native.process_transaction(transaction=transaction(identity="buy", amount=100, price=100))
    other = replace(SBER, sid=55, asset=replace(SBER.asset, id=55))
    native.process_transaction(transaction=transaction(identity="other", amount=100, price=100, asset=other))
    before = native.investments.history()
    with pytest.raises(expected_exception=ValueError, match="cash-in-lieu"):
        native.process_splits(
            splits=[Split(
                asset=SBER.asset,
                effective_date=START.date(),
                ratio=0.5,
            ), Split(asset=other.asset, effective_date=START.date(), ratio=3)],
        )
    assert native.investments.history() == before
    assert native.position_tracker.positions[KEY].amount == 100
    split = Split(asset=SBER.asset, effective_date=START.date(), ratio=0.5)
    native.process_splits(splits=[split])
    native.process_splits(splits=[split])
    assert native.position_tracker.positions[KEY].amount == 200
    result = native.investment_snapshot(currency="USD", dt=START)
    assert result.market_value == pytest.approx(expected=20000 / 82)
    assert result.unrealized_pnl == 0


def test_failed_opening_history_validation_does_not_initialize_the_book():
    native, reporting = ledger()
    native.process_transaction(transaction=transaction(identity="buy", amount=100, price=100))
    restored = Ledger(trading_sessions=pd.DatetimeIndex(data=[START, END]), data_frequency=datetime.timedelta(days=1))
    restored.position_tracker.update_position(
        asset=SBER,
        exchange_name="MOEX",
        trading_account_id="account",
        amount=99,
        cost_basis=100,
        last_sale_price=100,
        last_sale_date=START,
    )
    config = replace(reporting, investment_history=native.investments.history())
    with pytest.raises(expected_exception=ValueError, match="reconcile"):
        restored.enable_investment_accounting(reporting=config, dt=START, opening_cash={})
    assert restored.investments is None
    restored.position_tracker.update_position(
        asset=SBER,
        exchange_name="MOEX",
        trading_account_id="account",
        amount=100,
    )
    restored.enable_investment_accounting(reporting=config, dt=START, opening_cash={})
    assert restored.investment_snapshot(currency="USD", dt=START).historical_cost == pytest.approx(expected=10000 / 82)
