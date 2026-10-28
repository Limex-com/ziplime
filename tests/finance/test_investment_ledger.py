import datetime
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from tests.finance.investment_fixtures import accounting_ledger, SBER, KEY, START, END
from tests.finance.currency_fixtures import currency
from tests.bond_fixtures import make_zero_coupon_bond
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.split import Split
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.ledger import Ledger
from tests.finance.currency_fixtures import reporting_view
from ziplime.finance.domain.transaction import Transaction


def ledger(opening_cash=None):
    native = Ledger(trading_sessions=pd.DatetimeIndex(data=[START, END]), data_frequency=datetime.timedelta(days=1))
    reporting = CurrencyReporting(
        base_currency="EUR", currencies=("EUR", "USD"), conversion=accounting_ledger()._monetary_valuation.conversion, investment_pnl=True,
        opening_cash=({("MOEX", "account", "EUR"): 10000, ("MOEX", "account", "USD"): 150}
                      if opening_cash is None else opening_cash))
    native.enable_currency_reporting(base_currency="EUR", dt=START)
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
    usd = reporting_view(source=native, reporting=reporting, currency="USD", initial_dt=START)
    us = replace(SBER, sid=55, quote=currency(name="USD", identity=1))
    native.process_transaction(transaction=transaction(identity="sber", amount=100, price=100))
    native.process_transaction(transaction=transaction(identity="us", amount=5, price=30, asset=us))
    native.set_reporting_datetime(dt=END)
    for asset, price in ((SBER, 110), (us, 30)):
        native.position_tracker.update_position(
            asset=asset, exchange_name="MOEX", trading_account_id="account", last_sale_price=price, last_sale_date=END)
    await native.update_portfolio()
    usd.refresh(dt=END)
    assert native.cash_balances(dt=END) == {
        ("MOEX", "account", "EUR"): 0, ("MOEX", "account", "USD"): 0}
    assert native.portfolio.portfolio_value == pytest.approx(expected=41000)
    assert native.position_tracker.stats.net_value == pytest.approx(expected=41000)
    assert usd.portfolio.portfolio_value == 205
    assert usd.portfolio.pnl == pytest.approx(expected=55 - 10000 / 82)
    assert usd.investment.total_pnl == pytest.approx(expected=usd.portfolio.pnl)
    assert usd.investment.historical_cost == pytest.approx(expected=10000 / 82 + 150)


@pytest.mark.asyncio
async def test_commissions_are_fill_linked_and_idempotent():
    native, _ = ledger(opening_cash={
        ("MOEX", "account", "EUR"): 10082, ("MOEX", "account", "USD"): 150})
    executed = transaction(identity="sber", amount=100, price=100)
    native.process_transaction(transaction=executed)
    native.process_transaction(transaction=transaction(identity="sber", amount=100, price=100))
    commission = Commission(asset=SBER, order=None, amount=82, transaction=executed, dt=START, id="fee")
    native.process_commission(commission=commission, tr=None)
    native.process_commission(commission=commission, tr=None)
    await native.update_portfolio()
    assert native.investment_snapshot(currency="USD", dt=START).fees == 1
    assert native.investment_snapshot(currency="USD", dt=START).unrealized_pnl == pytest.approx(expected=-1)
    assert native.cash_balances(dt=START)[("MOEX", "account", "EUR")] == 0
    before = native.investment_history()
    with pytest.raises(expected_exception=ValueError, match="must already be recorded"):
        native.process_commission(commission=Commission(
            asset=SBER, order=None, amount=82, transaction=transaction("unknown", 100, 100)), tr=None)
    assert native.investment_history() == before


@pytest.mark.parametrize("other_scope", [("MOEX", "other-account"), ("OTHER", "account")])
def test_commissions_use_transaction_scope_when_fill_ids_repeat_and_orders_have_partial_fills(other_scope):
    native, _ = ledger(opening_cash={
        ("MOEX", "account", "EUR"): 3000, (*other_scope, "EUR"): 3000})
    first = transaction("same-id", 10, 100)
    first.order_id = "partial-order"
    partial = transaction("partial", 5, 120)
    partial.order_id = first.order_id
    other = replace(transaction("same-id", 20, 100),
                    exchange_name=other_scope[0], trading_account_id=other_scope[1])
    for executed in (first, partial, other):
        native.process_transaction(executed)

    fees = [Commission(asset=SBER, order=None, amount=6, transaction=partial, id="partial-fee"),
            Commission(asset=SBER, order=None, amount=20, transaction=other, id="other-fee")]
    for fee in fees:
        native.process_commission(fee, tr=None)
        native.process_commission(fee, tr=None)

    positions = {position.key: position for position in native.investment_snapshot("EUR", START).positions}
    assert positions[KEY].historical_cost == 1606
    assert positions[KEY].fees == 6
    assert positions[(*other_scope, SBER)].historical_cost == 2020
    assert positions[(*other_scope, SBER)].fees == 20
    assert native.cash_balances(START) == {
        ("MOEX", "account", "EUR"): 1394, (*other_scope, "EUR"): 980}


def test_commission_with_a_different_asset_from_its_transaction_cannot_change_accounting():
    native, _ = ledger()
    executed = transaction("buy", 10, 100)
    native.process_transaction(executed)
    before = native.investment_history()

    with pytest.raises(ValueError, match="asset must match"):
        native.process_commission(Commission(
            asset=replace(SBER, sid=55), order=None, amount=10, transaction=executed), tr=None)

    assert native.investment_history() == before


@pytest.mark.asyncio
async def test_currency_labelled_external_funding_is_not_pnl():
    native, reporting = ledger()
    usd = reporting_view(source=native, reporting=reporting, currency="USD", initial_dt=START)
    native.set_reporting_datetime(dt=END)
    await native.capital_change(
        change_amount=100, currency="USD", exchange_name="MOEX", trading_account_id="account", event_id="deposit")
    await native.capital_change(
        change_amount=100, currency="USD", exchange_name="MOEX", trading_account_id="account", event_id="deposit")
    await native.update_portfolio()
    usd.refresh(dt=END)
    assert native.cash_balances(dt=END)[("MOEX", "account", "USD")] == 250
    assert usd.portfolio.pnl == pytest.approx(expected=10000 / 200 - 10000 / 82)
    assert usd.investment.total_pnl == 0


@pytest.mark.asyncio
async def test_disposal_realizes_security_pnl_and_retained_cash_keeps_its_fx_exposure():
    native, reporting = ledger()
    usd = reporting_view(source=native, reporting=reporting, currency="USD", initial_dt=START)
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
    restored.enable_currency_reporting(base_currency="EUR", dt=START)
    with pytest.raises(expected_exception=ValueError, match="historical"):
        restored.enable_investment_accounting(reporting=reporting, dt=START, opening_cash=dict(reporting.opening_cash))
    config = replace(reporting, investment_history=native.investment_history())
    restored.enable_investment_accounting(reporting=config, dt=START, opening_cash=dict(config.opening_cash))
    assert restored.investment_snapshot(
        currency="USD",
        dt=START,
    ) == native.investment_snapshot(currency="USD", dt=START)


@pytest.mark.asyncio
async def test_dividend_entitlements_keep_account_scope_payment_currency_and_payment_time_fx_after_sale():
    native, reporting = ledger(opening_cash={
        ("MOEX", "account", "EUR"): 10000, ("MOEX", "account", "USD"): 150,
        ("MOEX", "short-account", "EUR"): 400, ("MOEX", "short-account", "USD"): 40})
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
        currency=currency(name="USD", identity=1),
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
    cash_flow_before_payment = native.portfolio.cash_flow
    native.set_reporting_datetime(dt=END)
    await native.process_dividends(next_session=END.date(), asset_service=service)
    await native.process_dividends(next_session=END.date(), asset_service=service)
    result = native.investment_snapshot(currency="USD", dt=END)
    assert result.income == 60
    assert [position.income for position in result.positions] == [100, -40]
    assert native.investment_snapshot(currency="EUR", dt=END).income == 60 * 200
    assert native.cash_balances(dt=END)[("MOEX", "short-account", "USD")] == 0
    assert native.cash_balances(dt=END)[("MOEX", "account", "USD")] == 250
    assert native.portfolio.cash_flow - cash_flow_before_payment == 60 * 200
    restored.set_reporting_datetime(dt=END)
    await restored.process_dividends(next_session=END.date(), asset_service=service)
    assert restored.investment_snapshot(currency="USD", dt=END) == result
    assert restored.cash_balances(dt=END) == native.cash_balances(dt=END)


@pytest.mark.asyncio
async def test_equal_dividends_in_different_currencies_post_once_to_each_exchange_and_account():
    native, _ = ledger(opening_cash={
        ("MOEX", "account", "EUR"): 1000, ("OTHER", "other-account", "EUR"): 2000})
    other_listing = replace(SBER, sid=55)
    native.process_transaction(transaction("first", amount=10, price=100))
    native.process_transaction(replace(
        transaction("second", amount=20, price=100, asset=other_listing),
        exchange_name="OTHER", trading_account_id="other-account"))
    payout = DividendPayout(
        asset=SBER.asset, amount=1, ex_date=START.date(), pay_date=END.date(),
        record_date=START.date(), declared_date=START.date(), currency=currency(name="USD", identity=1))
    service = SimpleNamespace(get_cash_dividends_with_ex_date=AsyncMock(
        return_value=[payout, replace(payout, currency=SBER.quote)]))
    await native.process_dividends(START.date(), service)
    await native.process_dividends(START.date(), service)
    before = native.portfolio.cash_flow
    native.set_reporting_datetime(END)
    await native.process_dividends(END.date(), service)
    await native.process_dividends(END.date(), service)

    assert native.cash_balances(END) == {
        ("MOEX", "account", "EUR"): 10, ("MOEX", "account", "USD"): 10,
        ("OTHER", "other-account", "EUR"): 20, ("OTHER", "other-account", "USD"): 20}
    assert native.portfolio.cash_flow - before == 30 * 200 + 30
    assert len(native.investment_events) == 6  # Two trades and four scoped income postings.
    assert native.investment_snapshot("USD", END).income == pytest.approx(30 + 30 / 200)


@pytest.mark.asyncio
async def test_plain_currency_reporting_converts_foreign_dividends_without_investment_history():
    native = Ledger(trading_sessions=pd.DatetimeIndex([START, END]), data_frequency=datetime.timedelta(days=1))
    native.enable_currency_reporting(base_currency="EUR", dt=START, conversion=accounting_ledger()._monetary_valuation.conversion)
    native.position_tracker.update_position(
        asset=SBER, exchange_name="MOEX", trading_account_id="account", amount=10,
        last_sale_price=100, last_sale_date=START)
    payout = DividendPayout(
        asset=SBER.asset, amount=1, ex_date=START.date(), pay_date=END.date(),
        record_date=START.date(), declared_date=START.date(), currency=currency(name="USD", identity=1))
    service = SimpleNamespace(get_cash_dividends_with_ex_date=AsyncMock(return_value=[payout]))
    await native.process_dividends(START.date(), service)
    native.set_reporting_datetime(END)
    await native.process_dividends(END.date(), service)
    await native.process_dividends(END.date(), service)

    assert native.native_currency_accounting is False
    assert native.portfolio.cash_flow == 10 * 200
    assert native.reporting_events == [(END, 2000, False)]


@pytest.mark.asyncio
async def test_coupon_entitlements_preserve_account_scope_and_history_after_sale():
    bond = make_zero_coupon_bond()
    native, reporting = ledger(opening_cash={
        ("MOEX", "account", "USD"): 2000, ("MOEX", "short-account", "USD"): 5})
    coupon = BondEvent(
        asset=bond.asset, event_type=BondEventType.COUPON, record_date=START.date(),
        date=END.date(), value=5, currency="USD")
    service = SimpleNamespace(get_bond_events=AsyncMock(return_value={bond.asset.id: [coupon]}))
    native.process_transaction(transaction("buy-bonds", amount=2, price=100, asset=bond))
    native.process_transaction(replace(
        transaction("short-bond", amount=-1, price=100, asset=bond), trading_account_id="short-account"))
    await native.process_bond_events(START.date(), service)
    await native.process_bond_events(START.date(), service)
    native.process_transaction(transaction("sell-bonds", amount=-2, price=100, asset=bond))
    native.process_transaction(replace(
        transaction("cover-bond", amount=1, price=100, asset=bond), trading_account_id="short-account"))
    history = native.investment_history()
    assert [event.amount for _, event in history.income_due] == [10, -5]
    restored = Ledger(trading_sessions=pd.DatetimeIndex([START, END]), data_frequency=datetime.timedelta(days=1))
    restored.enable_investment_accounting(
        reporting=replace(reporting, investment_history=history), dt=START, opening_cash={})

    before = native.portfolio.cash_flow
    for target in (native, restored):
        target.set_reporting_datetime(END)
        await target.process_bond_events(END.date(), service)
        await target.process_bond_events(END.date(), service)
        assert target.cash_balances(END) == {
            ("MOEX", "account", "USD"): 2010, ("MOEX", "short-account", "USD"): 0}
        assert target.investment_snapshot("USD", END).income == 5
        assert not target.position_tracker.income_due
    assert native.portfolio.cash_flow - before == 5 * 200
    assert restored.investment_snapshot("USD", END) == native.investment_snapshot("USD", END)


def test_splits_are_idempotent_and_reject_the_whole_batch_before_mutating_positions():
    native, _ = ledger(opening_cash={
        ("MOEX", "account", "EUR"): 20000, ("MOEX", "account", "USD"): 150})
    native.process_transaction(transaction=transaction(identity="buy", amount=100, price=100))
    other = replace(SBER, sid=55, asset=replace(SBER.asset, id=55))
    native.process_transaction(transaction=transaction(identity="other", amount=100, price=100, asset=other))
    before = native.investment_history()
    with pytest.raises(expected_exception=ValueError, match="cash-in-lieu"):
        native.process_splits(
            splits=[Split(
                asset=SBER.asset,
                effective_date=START.date(),
                ratio=0.5,
            ), Split(asset=other.asset, effective_date=START.date(), ratio=3)],
        )
    assert native.investment_history() == before
    assert native.position_tracker.positions[KEY].amount == 100
    split = Split(asset=SBER.asset, effective_date=START.date(), ratio=0.5)
    native.process_splits(splits=[split])
    native.process_splits(splits=[split])
    assert native.position_tracker.positions[KEY].amount == 200
    result = native.investment_snapshot(currency="USD", dt=START)
    assert result.market_value == pytest.approx(expected=20000 / 82)
    assert result.unrealized_pnl == 0


def test_failed_opening_history_validation_does_not_initialize_the_accounting_ledger():
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
    config = replace(reporting, investment_history=native.investment_history())
    with pytest.raises(expected_exception=ValueError, match="reconcile"):
        restored.enable_investment_accounting(reporting=config, dt=START, opening_cash={})
    assert restored.native_currency_accounting is False
    restored.position_tracker.update_position(
        asset=SBER,
        exchange_name="MOEX",
        trading_account_id="account",
        amount=100,
    )
    restored.enable_investment_accounting(reporting=config, dt=START, opening_cash={})
    assert restored.investment_snapshot(currency="USD", dt=START).historical_cost == pytest.approx(expected=10000 / 82)
