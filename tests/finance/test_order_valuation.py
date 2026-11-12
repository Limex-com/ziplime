import datetime
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import polars as pl
import pytest

from tests.finance.currency_fixtures import currency
from tests.finance.investment_fixtures import accounting_ledger, SBER, START, END
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_price_basis import OrderPriceBasis
from ziplime.finance.domain.order_quantity_basis import OrderQuantityBasis
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import LimitOrder
from ziplime.finance.monetary_valuation_service import MonetaryValuationService
from ziplime.trading.trading_algorithm import TradingAlgorithm


@pytest.mark.asyncio
async def test_orders_are_comparable_without_metrics_and_preserve_quantity_and_price_bases():
    algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
    algorithm.currency_conversion = accounting_ledger()._monetary_valuation.conversion
    algorithm.currency_reporting = None
    algorithm._ledger = Ledger(
        trading_sessions=pd.DatetimeIndex(data=[START, END]),
        data_frequency=datetime.timedelta(days=1),
    )
    algorithm._monetary_valuation_service = MonetaryValuationService(
        conversion=algorithm.currency_conversion, bond_book=algorithm._ledger.bond_book)
    algorithm.simulation_dt = END
    algorithm.clock = SimpleNamespace(
        trading_calendar=SimpleNamespace(tz=datetime.timezone.utc), start_session=START.date())
    us = replace(SBER, sid=55, quote=currency(name="USD", identity=1))
    exchange = SimpleNamespace(get_spot_value=AsyncMock(return_value=pl.DataFrame(data={
        "sid": [SBER.sid, us.sid], "date": [END, END], "close": [110.0, 30.0],
    })))
    algorithm.exchange_repository = SimpleNamespace(get_exchange_by_mic=AsyncMock(return_value=exchange))
    orders = [
        Order(
            id="eur",
            dt=START,
            asset=SBER,
            amount=2,
            filled=1,
            commission=0,
            execution_style=LimitOrder(limit_price=100),
            status=OrderStatus.OPEN,
            exchange_name="MOEX",
            trading_account_id="account",
        ),
        Order(
            id="usd",
            dt=START,
            asset=us,
            amount=-3,
            filled=-1,
            commission=0,
            execution_style=LimitOrder(limit_price=25),
            status=OrderStatus.OPEN,
            exchange_name="MOEX",
            trading_account_id="account",
        ),
    ]
    submitted = await algorithm.value_orders(orders=orders, currency="USD")
    assert [order.order_id for order in submitted.orders] == ["eur", "usd"]
    assert submitted.gross_value == pytest.approx(expected=91.1)
    assert submitted.net_value == pytest.approx(expected=-88.9)
    assert submitted.orders[0].price_basis is OrderPriceBasis.MARKET
    assert submitted.orders[0].quantity_basis is OrderQuantityBasis.SUBMITTED
    assert exchange.get_spot_value.await_count == 1
    remaining = await algorithm.value_orders(orders=orders, currency="USD", quantity_basis=OrderQuantityBasis.REMAINING)
    assert remaining.gross_value == pytest.approx(expected=60.55)
    assert remaining.net_value == pytest.approx(expected=-59.45)
    assert remaining.orders[0].quantity_basis is OrderQuantityBasis.REMAINING
    limit = await algorithm.value_orders(orders=orders, currency="USD", price_basis=OrderPriceBasis.LIMIT)
    assert limit.gross_value == 76
    assert limit.net_value == -74
    assert limit.orders[0].price_basis is OrderPriceBasis.LIMIT
    remaining_at_limit = await algorithm.value_orders(
        orders=orders, currency="USD", price_basis=OrderPriceBasis.LIMIT, quantity_basis=OrderQuantityBasis.REMAINING)
    assert remaining_at_limit.gross_value == pytest.approx(expected=50.5)
    assert remaining_at_limit.net_value == pytest.approx(expected=-49.5)
    assert remaining_at_limit.orders[0].price_basis is OrderPriceBasis.LIMIT
    assert remaining_at_limit.orders[0].quantity_basis is OrderQuantityBasis.REMAINING
    assert json.dumps(obj=[
        remaining_at_limit.orders[0].price_basis,
        remaining_at_limit.orders[0].quantity_basis,
    ]) == '["limit", "remaining"]'
    assert submitted.orders[0].monetary.reference_price == 110
    assert submitted.orders[0].monetary.fx.observed_at == END
    assert [order.amount for order in orders] == [2, -3]


@pytest.mark.asyncio
async def test_order_valuation_requires_a_configured_service():
    algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
    algorithm._monetary_valuation_service = None
    with pytest.raises(expected_exception=ValueError, match="Configure currency_conversion or CurrencyReporting"):
        await algorithm.value_orders(orders=[], currency="USD")
