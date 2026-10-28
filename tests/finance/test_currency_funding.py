from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.finance.currency_fixtures import currency
from tests.finance.currency_fixtures import equity_listing, reporting_tracker
from tests.finance.investment_fixtures import END, KEY, SBER, START, accounting_ledger, fill
from tests.finance.test_investment_ledger import ledger, transaction
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.domain.cash_event import CashEvent
from ziplime.finance.domain.cash_event_kind import CashEventKind
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.investment_event import InvestmentEvent
from ziplime.finance.domain.investment_event_kind import InvestmentEventKind
from ziplime.finance.domain.investment_history import InvestmentHistory
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import MarketOrder
from tests.finance.investment_fixtures import restore_ledger
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar
from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.vectorized.adapter import to_execution_result
from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals

import pandas as pd
import polars as pl


EUR = ("MOEX", "account", "EUR")
USD = ("MOEX", "account", "USD")
USD_ASSET = replace(SBER, sid=55, quote=currency(name="USD", identity=1))


def usd_fill():
    return replace(fill(identity="buy-usd", quantity=1, price=100),
                   key=(*KEY[:2], USD_ASSET), currency="USD")


@pytest.mark.parametrize("balances", [
    {EUR: 20000},
    {("MOEX", "other-account", "USD"): 100},
    {("OTHER", "account", "USD"): 100},
    {USD: 99},
])
def test_purchase_requires_its_own_funded_currency_and_account(balances):
    investments = accounting_ledger(opening_cash=balances)
    before = investments.investment_history()
    with pytest.raises(ValueError, match="Insufficient USD cash.*explicit currency exchange"):
        investments.record_investment_event(usd_fill())
    assert investments.investment_history() == before
    assert investments.investment_quantities == {}


def test_explicit_fx_execution_funds_purchase_and_debits_both_currencies_once():
    investments = accounting_ledger(opening_cash={EUR: 8200})
    investments.exchange_cash(
        identity="fx", dt=START, exchange="MOEX", account="account",
        source_currency="EUR", currency="USD", sold=8200, received=100)
    assert investments.record_investment_event(usd_fill())
    assert not investments.record_investment_event(usd_fill())
    assert investments.cash_balances(START) == {EUR: 0, USD: 0}


def test_unfunded_fx_execution_records_neither_cash_leg():
    investments = accounting_ledger(opening_cash={EUR: 81, USD: 100})
    before = investments.investment_history()
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        investments.exchange_cash(
            identity="fx", dt=START, exchange="MOEX", account="account",
            source_currency="EUR", currency="USD", sold=82, received=1)
    assert investments.investment_history() == before
    assert investments.cash_balances(START) == {EUR: 81, USD: 100}


def test_withdrawal_rejects_overdraft_and_allows_exact_balance_idempotently():
    investments = accounting_ledger(opening_cash={USD: 100})
    withdrawal = CashEvent(id="withdraw", dt=START, key=USD, amount=-101, kind=CashEventKind.EXTERNAL)
    before = investments.investment_history()
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        investments.record_cash(withdrawal)
    assert investments.investment_history() == before
    withdrawal = replace(withdrawal, amount=-100)
    assert investments.record_cash(withdrawal)
    assert not investments.record_cash(withdrawal)
    assert investments.cash_balances(START)[USD] == 0


def test_future_funding_does_not_pay_for_an_earlier_purchase():
    investments = accounting_ledger(opening_cash={})
    investments.record_cash(CashEvent(
        id="future", dt=END, key=USD, amount=100, kind=CashEventKind.EXTERNAL))
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        investments.record_investment_event(usd_fill())
    assert not investments.investment_events


def test_backdated_withdrawal_cannot_overdraw_a_later_posted_purchase():
    investments = accounting_ledger(opening_cash={EUR: 100})
    investments.record_investment_event(fill(identity="buy", quantity=1, price=100, dt=END))
    before = investments.investment_history()
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        investments.record_cash(CashEvent(
            id="backdated", dt=START, key=EUR, amount=-1, kind=CashEventKind.EXTERNAL))
    assert investments.investment_history() == before
    assert investments.cash_balances(END)[EUR] == 0


def test_negative_opening_cash_is_rejected():
    with pytest.raises(ValueError, match="Opening native cash balances must be nonnegative"):
        accounting_ledger(opening_cash={USD: -1})


@pytest.mark.parametrize("kind, amount", [
    (InvestmentEventKind.FEE, 1),
    (InvestmentEventKind.INCOME, -1),
])
def test_fees_and_income_liabilities_require_payment_currency_cash(kind, amount):
    investments = accounting_ledger(opening_cash={EUR: 100})
    investments.record_investment_event(fill(identity="buy", quantity=1, price=100))
    before = investments.investment_history()
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        investments.record_investment_event(InvestmentEvent(
            id="payment", dt=START, key=KEY, kind=kind, currency="USD",
            amount=amount, transaction_id="buy" if kind == InvestmentEventKind.FEE else None))
    assert investments.investment_history() == before
    assert investments.cash_balances(START) == {EUR: 0}


def test_restoration_preserves_posted_execution_amounts_without_rerunning_admission():
    investments = accounting_ledger(opening_cash={EUR: 101})
    investments.record_investment_event(fill(identity="buy", quantity=1, price=100))
    investments.record_investment_event(InvestmentEvent(
        id="fee", dt=START, key=KEY, kind=InvestmentEventKind.FEE,
        currency="EUR", amount=1, transaction_id="buy"))
    restored = restore_ledger(valuation=investments._monetary_valuation, history=investments.investment_history())
    assert restored.investment_history() == investments.investment_history()
    assert restored.cash_balances(START) == {EUR: 0}
    delayed_opening = replace(investments.cash_events[0], dt=END)
    restored = restore_ledger(
        valuation=investments._monetary_valuation,
        history=InvestmentHistory(events=investments.investment_events, cash_events=(delayed_opening,)))
    assert restored.cash_balances(START) == {EUR: -101}
    assert restored.cash_balances(END) == {EUR: 0}


def test_roundoff_is_zero_but_a_real_unfunded_tiny_debit_is_rejected():
    investments = accounting_ledger(opening_cash={EUR: 100})
    investments.record_investment_event(fill(identity="buy", quantity=3, price=100 / 3))
    assert investments.cash_balances(START)[EUR] == 0
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        investments.record_investment_event(replace(usd_fill(), unit_value=1e-12))


def test_already_executed_trade_and_fee_are_recorded_even_if_the_projection_was_exceeded():
    native, _ = ledger(opening_cash={EUR: 100})
    executed = replace(transaction(identity="buy", amount=1, price=100, dt=END), commission=1)
    native.process_transaction(executed)
    native.process_commission(Commission(
        asset=SBER, order=None, amount=1, transaction=executed, dt=END, id="fee"), tr=None)
    assert len(native.investment_events) == 2
    assert native.investment_quantities == {KEY: 1}
    assert native.cash_balances(END) == {EUR: -1}
    assert native._reporting_dt == END


def test_unfunded_income_batch_preserves_all_entitlements_for_retry():
    native, _ = ledger(opening_cash={EUR: 100})
    receipts = InvestmentEvent(
        id="dividend", dt=START, key=KEY, kind=InvestmentEventKind.INCOME, currency="USD", amount=10)
    liability = replace(receipts, key=("MOEX", "short-account", SBER), amount=-1)
    native.position_tracker.restore_income(income_due=[(END.date(), receipts), (END.date(), liability)])
    before = native.investment_history()
    native.set_reporting_datetime(END)
    with pytest.raises(ValueError, match="Insufficient USD cash.*short-account"):
        native.position_tracker.pay_income(END.date(), post=native._post_income)
    assert native.investment_history() == before
    assert not native.reporting_events
    native.record_cash(CashEvent(
        id="fund", dt=END, key=("MOEX", "short-account", "USD"), amount=1, kind=CashEventKind.EXTERNAL))
    native.position_tracker.pay_income(END.date(), post=native._post_income)
    assert not native.position_tracker.income_due
    assert native.cash_balances(END) == {
        EUR: 100, USD: 10, ("MOEX", "short-account", "USD"): 0}
    assert len(native.investment_events) == 2


def test_unavailable_payment_fx_preserves_the_entire_income_batch():
    native, _ = ledger(opening_cash={EUR: 100})
    dollars = InvestmentEvent(
        id="usd-income", dt=START, key=KEY, kind=InvestmentEventKind.INCOME, currency="USD", amount=10)
    pounds = replace(dollars, id="gbp-income", currency="GBP")
    native.position_tracker.restore_income(income_due=[(END.date(), dollars), (END.date(), pounds)])
    before = native.investment_history()
    native.set_reporting_datetime(END)

    with pytest.raises(ValueError, match="GBP->EUR"):
        native.position_tracker.pay_income(END.date(), post=native._post_income)

    assert native.investment_history() == before
    assert native.cash_balances(END) == {EUR: 100}
    assert native.portfolio.cash_flow == 0
    assert not native.reporting_events


def simulation(native, commission, currency_commission=None, currency_slippage=None):
    exchange = SimulationExchange(
        name="MOEX", country_code="US", trading_calendar=get_calendar("XNYS"), clock=None,
        cash_balance=100, equity_slippage=NoSlippage(), future_slippage=NoSlippage(),
        equity_commission=commission, future_commission=NoCommission(),
        currency_commission=currency_commission, currency_slippage=currency_slippage,
        account_id="account", is_default=True)
    exchange.get_spot_value = AsyncMock(return_value=pl.DataFrame({
        "sid": [SBER.sid], "date": [START], "close": [100.0], "volume": [1000000.0]}))
    return exchange


def order(identity):
    return Order(
        id=identity, dt=START, asset=SBER, amount=1, filled=0, commission=0,
        execution_style=MarketOrder(), status=OrderStatus.OPEN,
        exchange_name="MOEX", trading_account_id="account")


@pytest.mark.asyncio
async def test_simulated_funded_fill_and_fee_leave_zero_cash_and_keep_real_order_references():
    native, _ = ledger(opening_cash={EUR: 101})
    exchange = simulation(native, PerShare(cost=1, min_trade_cost=0))
    original = order("buy")
    transactions, commissions, closed = await exchange.get_transactions(
        {SBER: {original.id: original}}, current_dt=START, same_bar_execution=True)
    assert closed == [original]
    assert commissions[0].order is original
    assert commissions[0].transaction is transactions[0]
    assert commissions[0].transaction_id == transactions[0].id
    assert original.filled == 1 and original.commission == 1
    for fill_ in transactions:
        native.process_transaction(fill_)
    for fee in commissions:
        native.process_commission(fee, tr=None)
    await native.update_portfolio()
    assert native.cash_balances(START) == {EUR: 0}
    assert native.portfolio.cash == 0
    assert native.portfolio.pnl == -1


@pytest.mark.asyncio
async def test_failed_external_withdrawal_preserves_ledger_equity_and_reporting_flows():
    native, _ = ledger(opening_cash={EUR: 100})
    before = native.investment_history()
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        await native.capital_change(
            change_amount=-101, currency="EUR", exchange_name="MOEX", trading_account_id="account")
    assert native.investment_history() == before
    assert native.portfolio.cash == native.portfolio.portfolio_value == 100
    assert not native.reporting_events


@pytest.mark.asyncio
@pytest.mark.parametrize("investment_accounting", [False, True])
async def test_later_exchange_failure_keeps_earlier_successful_fills_reconciled(investment_accounting):
    native, _ = ledger(opening_cash={EUR: 101})
    if not investment_accounting:
        native.native_currency_accounting = False
    first_exchange = simulation(native, PerShare(cost=1, min_trade_cost=0))
    second_exchange = simulation(native, NoCommission())
    second_exchange.name = "OTHER"
    second_exchange.get_transactions = AsyncMock(side_effect=RuntimeError("venue offline"))
    first_order, second_order = order("first"), order("second")
    second_order.exchange_name = "OTHER"
    orders = {
        "MOEX": {SBER: {first_order.id: first_order}},
        "OTHER": {SBER: {second_order.id: second_order}},
    }
    algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
    algorithm._ledger = native
    algorithm.same_bar_execution = True
    algorithm.capital_changes = {}
    algorithm.metrics_tracker = SimpleNamespace(emission_rate=native.position_tracker.data_frequency)
    algorithm.exchange_repository = SimpleNamespace(
        get_all_exchanges=AsyncMock(return_value=[first_exchange, second_exchange]))
    algorithm.blotter = SimpleNamespace(
        get_open_orders=lambda exchange_name: orders[exchange_name],
        get_order_by_id=lambda identity, exchange_name: orders[exchange_name][SBER][identity],
        prune_orders=Mock(),
    )
    with pytest.raises(RuntimeError, match="venue offline"):
        async for _ in algorithm.every_bar(START, current_data=None, handle_data=AsyncMock()):
            pass
    assert first_order.filled == 1 and first_order.commission == 1 and second_order.filled == 0
    assert native.position_tracker.positions[KEY].amount == 1
    assert native.portfolio.cash == 0
    if investment_accounting:
        assert native.investment_quantities == {KEY: 1}
        assert native.cash_balances(START) == {EUR: 0}
    assert len(native.transactions()) == 1
    algorithm.blotter.prune_orders.assert_called_once_with(closed_orders=[first_order])


@pytest.mark.asyncio
@pytest.mark.parametrize("account", [None, "custom-replay-account"])
async def test_vectorized_replay_seeds_cash_in_the_actual_execution_account(account):
    tracker, _, _, closes = reporting_tracker()
    config = replace(tracker._currency_reporting, investment_pnl=True)
    index = pd.DatetimeIndex(closes)
    prices = pd.DataFrame({"TEST": [10., 11., 12.]}, index=index)
    entries = pd.DataFrame({"TEST": [True, False, False]}, index=index)
    source = simulate_signals(
        prices=prices, entries=entries, exits=entries & False, size=1, initial_cash=100,
        execution=ExecutionTiming.same_close(), assets={"TEST": equity_listing()})
    exchange = simulation(native=tracker._ledger, commission=NoCommission())
    kwargs = {} if account is None else {"trading_account_id": account}
    result = await to_execution_result(
        portfolio=source, listings={"TEST": equity_listing()}, prices=prices,
        trading_calendar=tracker._trading_calendar, exchange=exchange,
        currency_reporting=config, **kwargs)
    book_ = result.trading_algorithm._ledger
    expected = "vectorized_account" if account is None else account
    assert book_.cash_balances(closes[-1]) == {("MOEX", expected, "EUR"): 90}
    assert book_.investment_quantities == {("MOEX", expected, equity_listing()): 1}
    assert config.opening_cash is None
