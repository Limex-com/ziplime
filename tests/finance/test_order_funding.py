from unittest.mock import AsyncMock, Mock
from types import SimpleNamespace

import polars as pl
import pytest

from tests.finance.investment_fixtures import END, KEY, SBER, START
from tests.finance.test_currency_funding import EUR, USD, USD_ASSET, simulation
from tests.finance.test_investment_ledger import ledger, transaction
from tests.finance.currency_fixtures import currency, listing
from dataclasses import replace
from tests.bond_fixtures import make_zero_coupon_bond
from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_bond_turnover import PerBondTurnover
from ziplime.finance.domain.commission import Commission
from ziplime.finance.domain.order_status import OrderStatus
from tests.finance.investment_fixtures import restore_ledger
from ziplime.finance.execution import LimitOrder, MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm


def algorithm(cash=None, commission=None, live=True, currency_commission=None):
    native, reporting = ledger(opening_cash={EUR: 101} if cash is None else cash)
    model = PerShare(cost=1, min_trade_cost=0) if commission is None else commission
    model.calculate_for_asset = Mock(wraps=model.calculate_for_asset)
    exchange = simulation(native, model, currency_commission=currency_commission)
    exchange.live = live
    exchange.submit_order = AsyncMock(wraps=exchange.submit_order)
    exchange.submit_orders = AsyncMock(wraps=exchange.submit_orders)
    algo = TradingAlgorithm.__new__(TradingAlgorithm)
    algo._ledger = native
    algo.currency_reporting = reporting
    algo.currency_conversion = reporting.conversion
    algo._monetary_valuation_service = native._monetary_valuation
    algo.automatic_currency_exchange = False
    algo._in_before_trading_start = False
    algo.initialized = True
    algo.trading_controls = []
    algo._can_order_asset = lambda **kwargs: True
    algo.current_data = None
    algo.simulation_dt = START
    algo.clock = SimpleNamespace(trading_calendar=exchange.trading_calendar, start_session=START.date())
    algo._logger = Mock()
    algo.new_orders = {}
    algo.blotter = InMemoryBlotter(exchanges=[exchange], cancel_policy=None)
    algo.exchange_repository = SimpleNamespace(
        get_default_exchange=AsyncMock(return_value=exchange),
        get_exchange_by_mic=AsyncMock(return_value=exchange),
        get_all_exchanges=AsyncMock(return_value=[exchange]),
    )
    return algo, exchange, model


@pytest.mark.asyncio
@pytest.mark.parametrize("live", [False, True])
async def test_projected_commission_is_checked_before_any_order_is_submitted(live):
    algo, exchange, model = algorithm(cash={EUR: 100}, live=live)
    before = algo._ledger.investment_history()
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        await algo.order(SBER, 1, MarketOrder())
    model.calculate_for_asset.assert_called_once_with(asset=SBER, quantity=1, transaction_amount=100)
    exchange.submit_order.assert_not_awaited()
    assert not algo.blotter.get_open_orders("MOEX")
    assert algo._ledger.investment_history() == before


@pytest.mark.asyncio
async def test_exact_projected_native_cost_is_admitted_without_posting_cash():
    algo, exchange, model = algorithm()
    submitted = await algo.order(SBER, 1, MarketOrder())
    exchange.submit_order.assert_awaited_once()
    model.calculate_for_asset.assert_called_once_with(asset=SBER, quantity=1, transaction_amount=100)
    assert algo._ledger.cash_balances(START) == {EUR: 101}
    assert not algo._ledger.transactions()
    assert submitted.submitted_valuations["USD"].order_id == submitted.id
    assert submitted.id in algo.blotter.get_open_orders("MOEX")[SBER]


@pytest.mark.asyncio
async def test_payment_currency_is_used_for_projected_commission_and_funding():
    algo, exchange, model = algorithm(cash={EUR: 20000, USD: 100})
    exchange.get_spot_value.return_value = pl.DataFrame({
        "sid": [USD_ASSET.sid], "date": [START], "close": [100.], "volume": [1e6]})
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        await algo.order(USD_ASSET, 1, MarketOrder())
    model.calculate_for_asset.assert_called_once_with(asset=USD_ASSET, quantity=1, transaction_amount=100)
    exchange.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_currency_exchange_funds_an_order_including_its_projected_fee():
    algo, exchange, _ = algorithm(cash={EUR: 8282})
    exchange.get_spot_value.return_value = pl.DataFrame({
        "sid": [USD_ASSET.sid], "date": [START], "close": [100.], "volume": [1e6]})
    with pytest.raises(ValueError, match="Insufficient USD cash"):
        await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_order.assert_not_awaited()
    await algo.exchange_currency(
        source_currency="EUR", currency="USD", sold=8282, received=101, execution_id="fx")
    await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_order.assert_awaited_once()
    assert algo._ledger.cash_balances(START) == {EUR: 0, USD: 101}


@pytest.mark.asyncio
async def test_buy_limit_uses_the_limit_budget_instead_of_the_lower_market_price():
    algo, exchange, model = algorithm(cash={EUR: 110.5})
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        await algo.order(SBER, 1, LimitOrder(110))
    model.calculate_for_asset.assert_called_once_with(asset=SBER, quantity=1, transaction_amount=110)
    exchange.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_sell_proceeds_cannot_fund_another_order():
    algo, exchange, _ = algorithm(cash={EUR: 100})
    await algo.order(SBER, -1, MarketOrder())
    with pytest.raises(ValueError, match="Insufficient EUR cash"):
        await algo.order(SBER, 1, MarketOrder())
    assert exchange.submit_order.await_count == 1


@pytest.mark.asyncio
async def test_live_execution_after_a_lost_submission_response_is_still_fully_accounted_for():
    algo, exchange, _ = algorithm()
    exchange.submit_order.side_effect = TimeoutError("broker response was lost")
    with pytest.raises(TimeoutError):
        await algo.order(SBER, 1, MarketOrder())
    original = exchange.submit_order.call_args.kwargs["order"]
    fill = transaction("confirmed-fill", 1, 100)
    fill.order_id = original.id
    fee = Commission(asset=SBER, order=None, amount=1, transaction=fill, dt=START)
    algo._process_execution_batch([fill], [fee])
    assert algo._ledger.investment_quantities == {KEY: 1}
    assert algo._ledger.cash_balances(START) == {EUR: 0}
    assert len(algo._ledger.investment_events) == 2


@pytest.mark.asyncio
async def test_broker_rejected_order_does_not_keep_cash_committed():
    algo, exchange, _ = algorithm()

    async def reject(order):
        order.reject("broker refusal")
        return order

    exchange.submit_order.side_effect = reject
    submitted = await algo.order(SBER, 1, MarketOrder())
    assert not submitted.open
    assert algo._ledger.cash_balances(START) == {EUR: 101}


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [float("nan"), float("inf")])
async def test_nonfinite_commission_projection_never_reaches_the_broker(cost):
    algo, exchange, model = algorithm()
    model.calculate_for_asset.return_value = cost
    model.calculate_for_asset.side_effect = None
    with pytest.raises(ValueError, match="Projected commission must be finite"):
        await algo.order(SBER, 1, MarketOrder())
    exchange.submit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_already_executed_live_fills_are_recorded_when_actual_cost_exceeds_the_projection():
    algo, exchange, _ = algorithm()
    submitted = await algo.order(SBER, 1, MarketOrder())
    submitted.filled, submitted.commission = 1, 1
    fill = transaction("actual", 1, 102, dt=END)
    fill.order_id = submitted.id
    fee = Commission(asset=SBER, order=submitted, amount=1, transaction=fill, dt=END)
    exchange.get_transactions = AsyncMock(return_value=([fill], [fee], [submitted]))
    algo.capital_changes = {}
    algo.same_bar_execution = True
    algo.metrics_tracker = SimpleNamespace(emission_rate=algo._ledger.position_tracker.data_frequency)
    async for _ in algo.every_bar(END, current_data=None, handle_data=AsyncMock()):
        pass
    assert algo._ledger.investment_quantities == {KEY: 1}
    assert algo._ledger.cash_balances(END) == {EUR: -2}
    assert len(algo._ledger.investment_events) == 2


@pytest.mark.asyncio
async def test_bond_commission_projection_receives_dirty_money_in_the_payment_currency():
    algo, exchange, model = algorithm(cash={USD: 1000}, commission=PerBondTurnover(cost=0.01))
    bond = make_zero_coupon_bond()
    exchange.commission_models[type(bond.asset)] = model
    algo.asset_service = SimpleNamespace()
    exchange.get_spot_value.return_value = pl.DataFrame({
        "sid": [bond.sid], "date": [START], "close": [98.], "volume": [1e6]})
    await algo.order(bond, 1, MarketOrder())
    model.calculate_for_asset.assert_called_once_with(asset=bond, quantity=1, transaction_amount=980)
    exchange.submit_order.assert_awaited_once()


@pytest.mark.asyncio
async def test_live_account_override_cannot_validate_one_account_and_submit_through_another():
    algo, exchange, _ = algorithm(cash={EUR: 1000, ("MOEX", "other", "EUR"): 1000})
    with pytest.raises(ValueError, match="exchange configured for the requested live trading account"):
        await algo.order(SBER, 1, MarketOrder(), account_id="other")
    exchange.submit_order.assert_not_awaited()


def automatic_fx_algorithm(cash, inverse=False, fx_commission=None):
    algo, exchange, model = algorithm(cash=cash, live=False, currency_commission=fx_commission)
    algo.automatic_currency_exchange = True
    pair = replace(listing(asset=currency("USD", 1), quote=currency("EUR", 2)), symbol="USDEUR")
    if inverse:
        pair = replace(pair, asset=pair.quote, quote=pair.asset, symbol="EURUSD")
    algo.asset_service = SimpleNamespace(get_asset_by_sid=AsyncMock(return_value=pair))

    async def prices(assets, **kwargs):
        return pl.DataFrame({
            "sid": [asset.sid for asset in assets], "date": [algo.simulation_dt] * len(assets),
            "close": [(1 / 82 if inverse else 82.0) if asset == pair else 100.0 for asset in assets],
            "volume": [1e6] * len(assets)})

    exchange.get_spot_value.side_effect = prices
    return algo, exchange, pair


async def process_bar(algo):
    algo.capital_changes = {}
    algo.same_bar_execution = True
    algo.metrics_tracker = SimpleNamespace(emission_rate=algo._ledger.position_tracker.data_frequency)
    async for _ in algo.every_bar(algo.simulation_dt, current_data=None, handle_data=AsyncMock()):
        pass


@pytest.mark.asyncio
async def test_auto_fx_submits_one_batch_with_currency_first_and_both_orders_in_the_blotter():
    algo, exchange, pair = automatic_fx_algorithm({EUR: 10000})
    requested = await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_orders.assert_awaited_once()
    fx, accepted = exchange.submit_orders.call_args.kwargs["orders"]
    assert accepted is requested
    assert fx.asset == pair and fx.amount == 101
    assert fx.status == requested.status == OrderStatus.OPEN
    assert algo.get_order(requested.id, "MOEX") is requested
    assert algo.get_order(fx.id, "MOEX") is fx
    assert algo.get_open_orders(USD_ASSET) == [requested]
    assert algo.get_open_orders(pair) == [fx]
    assert algo._ledger.cash_balance("USD", "MOEX", "account") == 0
    assert not algo._ledger.transactions()
    for order in (fx, requested):
        assert all(value.order_id == order.id for value in order.submitted_valuations.values())

    await process_bar(algo)
    exchange.submit_orders.assert_awaited_once()
    assert requested.status == fx.status == OrderStatus.FILLED
    assert algo._ledger.cash_balance("USD", "MOEX", "account") == 0
    # 101 USD * 82 EUR. Currency trades have their own free default model.
    assert algo._ledger.cash_balance("EUR", "MOEX", "account") == 1718
    assert algo._ledger.investment_quantities == {("MOEX", "account", USD_ASSET): 1}
    assert not algo.get_open_orders()
    history = algo._ledger.investment_history()
    restored = restore_ledger(history=history, valuation=algo._ledger._monetary_valuation)
    assert restored.cash_balances(START) == algo._ledger.cash_balances(START)
    await algo._ledger.update_portfolio()
    assert algo._ledger.investment_snapshot("USD", START).fees == pytest.approx(1)


@pytest.mark.asyncio
async def test_auto_fx_inverse_pair_includes_fee_in_the_currency_being_acquired():
    from ziplime.finance.commission.per_trade import PerTrade
    algo, exchange, pair = automatic_fx_algorithm({EUR: 10000}, inverse=True, fx_commission=PerTrade(cost=1))
    requested = await algo.order(USD_ASSET, 1, MarketOrder())
    fx, accepted = exchange.submit_orders.call_args.kwargs["orders"]
    assert accepted is requested and fx.asset == pair and fx.amount == -8364
    await process_bar(algo)
    assert fx.status == requested.status == OrderStatus.FILLED
    assert algo._ledger.cash_balance("USD", "MOEX", "account") == pytest.approx(0)
    assert algo._ledger.cash_balance("EUR", "MOEX", "account") == 1636
    exchange.submit_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_auto_fx_default_does_not_charge_the_equity_commission_on_currency_units():
    algo, exchange, _ = automatic_fx_algorithm({EUR: 8282})
    requested = await algo.order(USD_ASSET, 1, MarketOrder())
    await process_bar(algo)

    assert requested.status == OrderStatus.FILLED
    assert algo._ledger.cash_balance("EUR", "MOEX", "account") == 0
    assert algo._ledger.cash_balance("USD", "MOEX", "account") == 0
    assert algo._ledger.investment_snapshot("USD", START).fees == 1
    exchange.submit_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_auto_fx_requires_sufficient_source_cash_including_its_commission():
    from ziplime.finance.commission.per_trade import PerTrade
    algo, exchange, _ = automatic_fx_algorithm({EUR: 8282}, fx_commission=PerTrade(cost=1))
    with pytest.raises(ValueError, match="no funded currency pair"):
        await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_order.assert_not_awaited()
    exchange.submit_orders.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_auto_fx_requires_manual_funding_before_submitting_any_order():
    algo, exchange, _ = automatic_fx_algorithm({EUR: 10000})
    exchange.live = True
    with pytest.raises(ValueError, match="Fund the quote currency"):
        await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_order.assert_not_awaited()
    exchange.submit_orders.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelling_a_batched_asset_order_uses_the_normal_blotter():
    algo, exchange, pair = automatic_fx_algorithm({EUR: 10000})
    requested = await algo.order(USD_ASSET, 1, MarketOrder())
    fx, _ = exchange.submit_orders.call_args.kwargs["orders"]
    await algo.cancel_order(requested.id, "MOEX")
    assert requested.status == OrderStatus.CANCELLED
    assert fx.status == OrderStatus.OPEN
    assert not algo.get_open_orders(USD_ASSET)
    assert algo.get_open_orders(pair) == [fx]
    exchange.submit_orders.assert_awaited_once()


@pytest.mark.asyncio
async def test_simulation_processes_fx_first_even_if_the_blotter_groups_the_asset_first():
    algo, exchange, pair = automatic_fx_algorithm({EUR: 10000})
    requested = await algo.order(USD_ASSET, 1, MarketOrder())
    fx, _ = exchange.submit_orders.call_args.kwargs["orders"]
    transactions, commissions, closed = await exchange.get_transactions(
        orders={USD_ASSET: {requested.id: requested}, pair: {fx.id: fx}},
        current_dt=START, same_bar_execution=True)
    assert [transaction.asset for transaction in transactions] == [pair, USD_ASSET]
    algo._process_execution_batch(transactions, commissions)
    assert algo._ledger.cash_balance("USD", "MOEX", "account") == 0
    assert algo._ledger.cash_balance("EUR", "MOEX", "account") == 1718
    assert closed == [fx, requested]


@pytest.mark.asyncio
async def test_fx_trading_controls_are_checked_before_submitting_the_batch():
    algo, exchange, pair = automatic_fx_algorithm({EUR: 10000})

    async def validate(asset, **kwargs):
        if asset == pair:
            raise ValueError("FX order exceeds its trading limit")

    algo.trading_controls = [SimpleNamespace(validate=validate)]
    with pytest.raises(ValueError, match="FX order exceeds"):
        await algo.order(USD_ASSET, 1, MarketOrder())
    exchange.submit_order.assert_not_awaited()
    exchange.submit_orders.assert_not_awaited()
    assert not algo.get_open_orders()
