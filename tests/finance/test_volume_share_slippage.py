import datetime
from unittest.mock import AsyncMock, Mock

import polars as pl
import pytest

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.errors import LiquidityExceeded
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import LimitOrder, MarketOrder
from ziplime.finance.slippage.volume_share_slippage import VolumeShareSlippage


@pytest.fixture
def dt():
    return datetime.datetime(2026, 9, 25, 14, tzinfo=datetime.timezone.utc)


@pytest.fixture
def asset():
    return Mock(spec=ExchangeAsset, symbol="TEST")


@pytest.fixture
def exchange():
    exchange = Mock(spec=Exchange)
    exchange.name = "TEST"
    exchange.get_spot_value = AsyncMock(
        return_value=pl.DataFrame({"open": [90.0], "close": [100.0], "volume": [1000.0]}),
    )
    return exchange


@pytest.fixture
def order(dt, asset):
    return Order(
        id="order", dt=dt, asset=asset, amount=100, filled=0, commission=0.0,
        execution_style=MarketOrder(), status=OrderStatus.OPEN,
        exchange_name="TEST", trading_account_id="account",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("price", [None, 90.0, 0.0])
async def test_process_order_uses_exchange_data_and_optional_execution_price(exchange, dt, order, direction, price):
    order.amount *= direction
    order.direction = float(direction)
    model = VolumeShareSlippage(volume_limit=0.025, price_impact=0.1)

    execution_price, amount = await model.process_order(exchange, dt, order, price=price)

    base_price = 100.0 if price is None else price
    assert execution_price == pytest.approx(base_price * (1 + direction * 0.1 * 0.025 ** 2))
    assert amount == 25 * direction
    exchange.get_spot_value.assert_awaited_once_with(
        assets=frozenset({order.asset}), fields=frozenset({"volume", "close"}), dt=dt,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_price", [None, float("nan")])
async def test_process_order_returns_no_fill_for_missing_close(exchange, dt, order, missing_price):
    exchange.get_spot_value.return_value = pl.DataFrame({"close": [missing_price], "volume": [1000.0]})

    assert await VolumeShareSlippage().process_order(exchange, dt, order) == (None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("volume", [0.0, 20.0])
async def test_process_order_raises_when_volume_cannot_fill_a_share(exchange, dt, order, volume):
    exchange.get_spot_value.return_value = pl.DataFrame({"close": [100.0], "volume": [volume]})

    with pytest.raises(LiquidityExceeded):
        await VolumeShareSlippage().process_order(exchange, dt, order)


@pytest.mark.asyncio
async def test_process_order_respects_limit_after_price_impact(exchange, dt, order):
    order.limit = LimitOrder(100.0).get_limit_price(is_buy=True)

    assert await VolumeShareSlippage().process_order(exchange, dt, order) == (None, None)


@pytest.mark.asyncio
async def test_simulate_uses_selected_price_and_cumulative_bar_volume(exchange, dt, asset, order):
    order.amount = 10
    second_order = Order(
        id="second", dt=dt, asset=asset, amount=100, filled=0, commission=0.0,
        execution_style=MarketOrder(), status=OrderStatus.OPEN,
        exchange_name="TEST", trading_account_id="account",
    )
    model = VolumeShareSlippage(volume_limit=0.025, price_impact=0.1)

    fills = [
        (filled_order, transaction)
        async for filled_order, transaction in model.simulate(
            exchange, frozenset({asset}), [order, second_order], dt,
            same_bar_execution=True, price_used_in_order_execution="open",
        )
    ]

    assert [filled_order for filled_order, _ in fills] == [order, second_order]
    assert [transaction.amount for _, transaction in fills] == [10, 15]
    assert [transaction.price for _, transaction in fills] == pytest.approx([
        90.0 * (1 + 0.1 * 0.01 ** 2),
        90.0 * (1 + 0.1 * 0.025 ** 2),
    ])
    assert model.volume_for_bar == 25
