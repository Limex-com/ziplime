"""Offline contract tests for the Lime Trader SDK live exchange.

The tests deliberately use the SDK's public DTOs while replacing all network
APIs with small async fakes.  ``asyncio.run`` keeps the suite independent of
pytest-asyncio and mirrors the synchronous test style used by this repository.
"""

from __future__ import annotations

import asyncio
import datetime
import inspect
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("lime_trader", reason="lime-trader-sdk must be installed")

from lime_trader.models.accounts import (  # noqa: E402
    AccountDetails,
    AccountPosition,
    AccountTrade,
    MarginType,
    RestrictionLevel,
    SecurityType,
    TradeSide,
)
from lime_trader.models.market import Period, Quote, QuoteHistory  # noqa: E402
from lime_trader.models.page import Page  # noqa: E402
from lime_trader.models.trading import (  # noqa: E402
    CancelOrderResponse,
    OrderDetails,
    OrderSide,
    OrderStatus as LimeOrderStatus,
    OrderType,
    PlaceOrderResponse,
    TimeInForce,
    ValidateOrderResponse,
)
from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.assets.entities.equity import Equity  # noqa: E402
from ziplime.assets.entities.exchange_asset import ExchangeAsset  # noqa: E402
from ziplime.assets.entities.exchange_info import ExchangeInfo  # noqa: E402
from ziplime.finance.domain.order import Order  # noqa: E402
from ziplime.finance.domain.order_status import OrderStatus  # noqa: E402
from ziplime.finance.execution import LimitOrder, MarketOrder  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402

from cross_asset_fixtures import USD  # noqa: E402

from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import (  # noqa: E402
    LimeTraderSdkExchange,
)


UTC = datetime.timezone.utc
ACCOUNT_NUMBER = "demo-account"
ORDER_RISK_ENV = {
    "ZIPLIME_ALLOWED_SYMBOLS": "AAPL,MSFT",
    "ZIPLIME_ALLOW_SELL": "false",
    "ZIPLIME_MAX_ORDER_QUANTITY": "1",
    "ZIPLIME_MAX_ORDER_NOTIONAL_USD": "500",
    "ZIPLIME_MAX_ORDERS_PER_INVOCATION": "1",
}


@pytest.fixture(autouse=True)
def _clear_aws_order_guards(monkeypatch):
    monkeypatch.delenv("ZIPLIME_ORDER_DEADLINE_UTC", raising=False)
    monkeypatch.delenv("ZIPLIME_REQUIRE_ORDER_RISK_LIMITS", raising=False)
    for name in ORDER_RISK_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def required_order_risk_limits(monkeypatch):
    monkeypatch.setenv("ZIPLIME_REQUIRE_ORDER_RISK_LIMITS", "true")
    for name, value in ORDER_RISK_ENV.items():
        monkeypatch.setenv(name, value)


def _dt(hour: int, minute: int = 0) -> datetime.datetime:
    return datetime.datetime(2026, 8, 20, hour, minute, tzinfo=UTC)


def _asset(symbol: str = "AAPL", sid: int = 1, mic: str = "XNGS") -> ExchangeAsset:
    start = datetime.date(1980, 1, 1)
    return ExchangeAsset(
        sid=sid,
        symbol=symbol,
        start_date=start,
        end_date=None,
        first_traded=start,
        auto_close_date=None,
        external_id=f"{symbol}@{mic}",
        exchange=ExchangeInfo(
            mic=mic,
            name="NASDAQ",
            canonical_name="NASDAQ",
            country_code="US",
        ),
        asset=Equity(
            id=sid,
            isin=None,
            asset_name=symbol,
            start_date=start,
            end_date=None,
            first_traded=start,
            auto_close_date=None,
        ),
        quote=USD,
    )


def _balance(account_number: str = ACCOUNT_NUMBER) -> AccountDetails:
    return AccountDetails(
        account_number=account_number,
        trade_platform="transaq",
        margin_type=MarginType.CASH,
        restriction=RestrictionLevel.NONE,
        daytrades_count=0,
        account_value_total=Decimal("12500.00"),
        cash=Decimal("10000.00"),
        day_trading_buying_power=Decimal("10000.00"),
        margin_buying_power=Decimal("10000.00"),
        non_margin_buying_power=Decimal("9000.00"),
        position_market_value=Decimal("2500.00"),
        unsettled_cash=Decimal("0.00"),
        cash_to_withdraw=Decimal("8000.00"),
    )


def _position(
    symbol: str = "AAPL", quantity: int = 10, current_price: str = "150.25"
) -> AccountPosition:
    return AccountPosition(
        symbol=symbol,
        quantity=quantity,
        average_open_price=Decimal("140.00"),
        current_price=Decimal(current_price),
        security_type=SecurityType.COMMON_STOCK,
    )


def _details(
    *,
    order_id: str = "broker-order-1",
    client_order_id: str = "client1",
    side: OrderSide = OrderSide.BUY,
    status: LimeOrderStatus = LimeOrderStatus.NEW,
    quantity: str = "5",
    executed_quantity: str = "0",
    order_type: OrderType = OrderType.MARKET,
    price: str = "0",
    executed_timestamp: datetime.datetime | None = None,
) -> OrderDetails:
    return OrderDetails(
        account_number=ACCOUNT_NUMBER,
        client_id=order_id,
        exchange="auto",
        quantity=Decimal(quantity),
        executed_quantity=Decimal(executed_quantity),
        order_status=status,
        price=Decimal(price),
        time_in_force=TimeInForce.DAY,
        order_type=order_type,
        order_side=side,
        symbol="AAPL",
        client_order_id=client_order_id,
        executed_timestamp=executed_timestamp,
    )


def _ziplime_order(
    asset: ExchangeAsset,
    *,
    order_id: str = "client-order/with-unsafe_chars",
    amount: int = 5,
    style=None,
    exchange_order_id: str | None = None,
) -> Order:
    return Order(
        id=order_id,
        dt=_dt(14, 0),
        asset=asset,
        amount=amount,
        filled=0,
        commission=0.0,
        execution_style=style or MarketOrder(),
        status=OrderStatus.OPEN,
        exchange_name="LIME",
        trading_account_id=ACCOUNT_NUMBER,
        exchange_order_id=exchange_order_id,
    )


class FakeAssetService:
    def __init__(self, *assets: ExchangeAsset):
        self.assets = {(asset.symbol, asset.mic): asset for asset in assets}
        self.requests: list[tuple[AssetSymbol, AssetType]] = []

    async def get_exchange_asset_by_symbol(
        self, *, symbol: AssetSymbol, asset_type: AssetType
    ) -> ExchangeAsset | None:
        self.requests.append((symbol, asset_type))
        return self.assets.get((symbol.symbol, symbol.mic))


class FakeAccountApi:
    def __init__(
        self,
        *,
        balances: list[AccountDetails] | None = None,
        positions: list[AccountPosition] | None = None,
        trades: list[AccountTrade] | None = None,
    ):
        self.balances = balances if balances is not None else [_balance()]
        self.positions = positions or []
        self.trades = trades or []
        self.balance_calls = 0
        self.position_calls: list[dict] = []
        self.trade_calls: list[dict] = []

    async def get_balances(self) -> list[AccountDetails]:
        self.balance_calls += 1
        return self.balances

    async def get_positions(self, **kwargs) -> list[AccountPosition]:
        self.position_calls.append(kwargs)
        return self.positions

    async def get_trades(self, **kwargs) -> Page[AccountTrade]:
        self.trade_calls.append(kwargs)
        return Page(
            data=self.trades,
            number=kwargs["page"].page,
            size=kwargs["page"].size,
            total_elements=len(self.trades),
        )


class FakeTradingApi:
    def __init__(self):
        self.validation_response = ValidateOrderResponse(is_valid=True)
        self.place_response = PlaceOrderResponse(success=True, data="broker-order-1")
        self.cancel_response = CancelOrderResponse(
            success=True, data="cancel-request-1"
        )
        self.active_orders: list[OrderDetails] = []
        self.details_by_id: dict[str, OrderDetails] = {}
        self.validated_orders = []
        self.placed_orders = []
        self.cancel_calls: list[dict] = []
        self.details_calls: list[str] = []

    async def validate_order(self, *, order):
        self.validated_orders.append(order)
        return self.validation_response

    async def place_order(self, *, order):
        self.placed_orders.append(order)
        return self.place_response

    async def get_active_orders(self, *, account_number: str):
        assert account_number == ACCOUNT_NUMBER
        return self.active_orders

    async def get_order_details(self, *, order_id: str):
        self.details_calls.append(order_id)
        return self.details_by_id[order_id]

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return self.cancel_response


class FakeMarketApi:
    def __init__(self):
        self.quotes: list[Quote] = []
        self.history_by_symbol: dict[str, list[QuoteHistory]] = {}
        self.history_by_window: dict[
            tuple[str, datetime.datetime, datetime.datetime], list[QuoteHistory]
        ] = {}
        self.current_quote_calls: list[list[str]] = []
        self.history_calls: list[dict] = []

    async def get_current_quotes(self, *, symbols: list[str]) -> list[Quote]:
        self.current_quote_calls.append(symbols)
        return self.quotes

    async def get_quotes_history(self, **kwargs) -> list[QuoteHistory]:
        self.history_calls.append(kwargs)
        window_key = (kwargs["symbol"], kwargs["from_date"], kwargs["to_date"])
        if window_key in self.history_by_window:
            return self.history_by_window[window_key]
        return self.history_by_symbol.get(kwargs["symbol"], [])


class FakeAsyncLimeClient:
    def __init__(
        self,
        *,
        balances: list[AccountDetails] | None = None,
        positions: list[AccountPosition] | None = None,
        trades: list[AccountTrade] | None = None,
    ):
        self.account = FakeAccountApi(
            balances=balances,
            positions=positions,
            trades=trades,
        )
        self.trading = FakeTradingApi()
        self.market = FakeMarketApi()


def _exchange(
    client: FakeAsyncLimeClient,
    asset_service: FakeAssetService,
    *,
    account_id: str | None = None,
    validate_orders: bool = True,
) -> LimeTraderSdkExchange:
    calendar = get_calendar("NYSE")
    return LimeTraderSdkExchange(
        name="LIME",
        canonical_name="Lime Trading",
        country_code="US",
        clock=SimpleNamespace(trading_calendar=calendar),
        trading_calendar=calendar,
        start_cash_balance=100_000.0,
        asset_service=asset_service,
        is_default=True,
        account_id=account_id,
        default_mic="XNGS",
        validate_orders=validate_orders,
        client=client,
    )


def test_class_is_concrete_and_constructor_uses_injected_client():
    asset = _asset()
    client = FakeAsyncLimeClient()

    exchange = _exchange(client, FakeAssetService(asset))

    assert not inspect.isabstract(LimeTraderSdkExchange)
    assert exchange._client is client
    assert exchange.name == "LIME"
    assert exchange.country_code == "US"
    assert exchange.is_default is True
    assert exchange.account_id == ""
    assert exchange.get_start_cash_balance() == 100_000.0
    assert exchange.is_alive() is True


def test_portfolio_and_positions_select_only_account_and_map_values():
    asset = _asset()
    client = FakeAsyncLimeClient(positions=[_position()])
    exchange = _exchange(client, FakeAssetService(asset))

    portfolio = asyncio.run(exchange.get_portfolio())

    assert exchange.account_id == ACCOUNT_NUMBER
    assert portfolio.cash == 10_000.0
    assert portfolio.portfolio_value == 12_500.0
    assert portfolio.positions_value == pytest.approx(1_502.5)
    assert portfolio.positions_exposure == pytest.approx(1_502.5)
    assert set(portfolio.positions) == {("LIME", ACCOUNT_NUMBER, asset)}
    position = portfolio.positions[("LIME", ACCOUNT_NUMBER, asset)]
    assert position.amount == 10
    assert position.cost_basis == 140.0
    assert position.last_sale_price == 150.25
    assert client.account.position_calls == [
        {"account_number": ACCOUNT_NUMBER, "date": None, "strategy": None}
    ]


def test_positions_carry_ledger_routing_fields():
    # ziplime's ledger reconciles broker positions by (exchange, account), so a
    # position without those fields cannot be synchronized at algorithm start.
    asset = _asset()
    client = FakeAsyncLimeClient(positions=[_position()])
    exchange = _exchange(client, FakeAssetService(asset))

    positions = asyncio.run(exchange.get_positions())

    position = positions[asset]
    assert position.exchange_name == "LIME"
    assert position.trading_account_id == ACCOUNT_NUMBER


def test_flat_positions_are_dropped():
    # Lime keeps a zero-quantity row for every symbol traded during the day;
    # ziplime's position stats overflow their index array on such a row.
    asset = _asset()
    client = FakeAsyncLimeClient(positions=[_position(quantity=0)])
    exchange = _exchange(client, FakeAssetService(asset))

    portfolio = asyncio.run(exchange.get_portfolio())

    assert portfolio.positions == {}
    assert portfolio.positions_value == 0.0
    assert portfolio.positions_exposure == 0.0


@pytest.mark.parametrize(
    ("amount", "style", "expected_side", "expected_type", "expected_price"),
    [
        (7, MarketOrder(), OrderSide.BUY, OrderType.MARKET, None),
        (
            -3,
            LimitOrder(limit_price=187.35),
            OrderSide.SELL,
            OrderType.LIMIT,
            Decimal("187.35"),
        ),
    ],
)
def test_submit_market_and_limit_orders_use_decimal_and_safe_client_id(
    amount, style, expected_side, expected_type, expected_price
):
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=amount, style=style)

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert order.exchange_order_id == "broker-order-1"
    assert order.trading_account_id == ACCOUNT_NUMBER
    assert exchange._orders_by_external_id == {"broker-order-1": order}
    assert len(client.trading.validated_orders) == 1
    assert len(client.trading.placed_orders) == 1
    submitted = client.trading.placed_orders[0]
    assert submitted is client.trading.validated_orders[0]
    assert submitted.account_number == ACCOUNT_NUMBER
    assert submitted.symbol == "AAPL"
    assert submitted.quantity == Decimal(abs(amount))
    assert submitted.side == expected_side
    assert submitted.order_type == expected_type
    assert submitted.price == expected_price
    assert submitted.exchange == "auto"
    assert submitted.time_in_force == TimeInForce.DAY
    assert submitted.client_order_id.isalnum()
    assert len(submitted.client_order_id) <= 32


def test_validation_failure_never_places_order():
    asset = _asset()
    client = FakeAsyncLimeClient()
    client.trading.validation_response = ValidateOrderResponse(
        is_valid=False,
        validation_message="insufficient buying power",
    )
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)

    with pytest.raises(ValueError, match="insufficient buying power"):
        asyncio.run(exchange.submit_order(_ziplime_order(asset)))

    assert len(client.trading.validated_orders) == 1
    assert client.trading.placed_orders == []


def test_zero_quantity_submit_never_reaches_the_broker():
    """Skipped, not raised: see test_a_zero_quantity_order_is_skipped_without_raising."""
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)

    asyncio.run(exchange.submit_order(_ziplime_order(asset, amount=0)))

    assert client.account.balance_calls == 0
    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []


@pytest.mark.parametrize("validate_orders", [True, False])
def test_expired_order_deadline_rejects_before_validate_or_place(
    monkeypatch, validate_orders
):
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(
        client,
        FakeAssetService(asset),
        account_id=ACCOUNT_NUMBER,
        validate_orders=validate_orders,
    )
    monkeypatch.setenv("ZIPLIME_ORDER_DEADLINE_UTC", "2000-01-01T00:00:00+00:00")

    with pytest.raises(RuntimeError, match="order deadline expired"):
        asyncio.run(exchange.submit_order(_ziplime_order(asset)))

    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []


def _assert_risk_rejected_before_broker_calls(exchange, client, order) -> None:
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(exchange.submit_order(order))

    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []


def test_required_risk_limits_allow_aapl_buy_one_limit_300(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert len(client.trading.validated_orders) == 1
    assert len(client.trading.placed_orders) == 1
    submitted = client.trading.placed_orders[0]
    assert submitted.symbol == "AAPL"
    assert submitted.side == OrderSide.BUY
    assert submitted.quantity == Decimal("1")
    assert submitted.order_type == OrderType.LIMIT
    assert submitted.price == Decimal("300.0")


def test_required_risk_limits_reject_unlisted_symbol_before_broker(
    required_order_risk_limits,
):
    asset = _asset("TSLA")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


@pytest.mark.parametrize("allowlist", [None, "", "*"])
def test_an_unpinned_allowlist_lets_any_symbol_through(
    required_order_risk_limits, monkeypatch, allowlist
):
    """A deploy-time symbol pin does not belong on a multi-tenant worker.

    Each deployment brings its own universe, so pinning the image to one ticker
    silently rejected every order of every other strategy — while the tick still
    reported success.
    """
    if allowlist is None:
        monkeypatch.delenv("ZIPLIME_ALLOWED_SYMBOLS", raising=False)
    else:
        monkeypatch.setenv("ZIPLIME_ALLOWED_SYMBOLS", allowlist)

    asset = _asset("NVDA")  # deliberately not the pinned AAPL
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=LimitOrder(limit_price=300.0))

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert len(client.trading.placed_orders) == 1
    assert client.trading.placed_orders[0].symbol == "NVDA"


def test_an_explicit_allowlist_still_pins_the_symbol(
    required_order_risk_limits, monkeypatch
):
    monkeypatch.setenv("ZIPLIME_ALLOWED_SYMBOLS", "AAPL")
    asset = _asset("NVDA")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=LimitOrder(limit_price=300.0))

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_the_other_risk_limits_survive_an_unpinned_allowlist(
    required_order_risk_limits, monkeypatch
):
    """Dropping the symbol pin must not drop quantity, notional or shorting."""
    monkeypatch.setenv("ZIPLIME_ALLOWED_SYMBOLS", "*")
    asset = _asset("NVDA")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)

    for order in (
        _ziplime_order(asset, amount=-1, style=LimitOrder(limit_price=300.0)),
        _ziplime_order(asset, amount=99, style=LimitOrder(limit_price=300.0)),
    ):
        _assert_risk_rejected_before_broker_calls(exchange, client, order)


def _market_quote(symbol: str, ask: str, bid: str | None = None) -> Quote:
    return Quote(
        symbol=symbol,
        ask=Decimal(ask),
        ask_size=Decimal("10"),
        bid=Decimal(bid or ask),
        bid_size=Decimal("10"),
        last=Decimal(ask),
        last_size=Decimal("1"),
        volume=1_000,
        date=_dt(15),
        high=Decimal(ask),
        low=Decimal(ask),
        open=Decimal(ask),
        close=Decimal(ask),
        week52_high=Decimal(ask),
        week52_low=Decimal(ask),
        change=Decimal("0"),
        change_pc=Decimal("0"),
    )


def test_a_market_order_within_the_notional_cap_reaches_the_broker(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    client.market.quotes = [_market_quote("AAPL", "300.00")]  # 1 x 300 <= 500
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=MarketOrder())

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert len(client.trading.placed_orders) == 1
    assert client.trading.placed_orders[0].order_type == OrderType.MARKET


def test_a_market_order_is_still_bounded_by_the_notional_cap(
    required_order_risk_limits,
):
    """An unpriced order must not slip past the one limit that caps money."""
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    client.market.quotes = [_market_quote("AAPL", "900.00")]  # 1 x 900 > 500
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=MarketOrder())

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_a_market_order_is_refused_when_the_quote_is_missing(
    required_order_risk_limits,
):
    """Fail closed: without a quote the exposure cannot be bounded at all."""
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    client.market.quotes = []
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=MarketOrder())

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_a_market_buy_is_priced_at_the_ask_it_will_cross(
    required_order_risk_limits,
):
    """Pricing a buy at the bid would under-count the exposure it takes on."""
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    # Bid is inside the cap, the ask the buy actually crosses is not.
    client.market.quotes = [_market_quote("AAPL", ask="600.00", bid="400.00")]
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=MarketOrder())

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_required_risk_limits_reject_sell_before_broker(required_order_risk_limits):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        amount=-1,
        style=LimitOrder(limit_price=300.0),
    )

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_required_risk_limits_reject_quantity_above_max_before_broker(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        amount=2,
        style=LimitOrder(limit_price=200.0),
    )

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_required_risk_limits_reject_market_order_without_notional_price(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=MarketOrder())

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_required_risk_limits_reject_limit_notional_above_max_before_broker(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        amount=1,
        style=LimitOrder(limit_price=500.01),
    )

    _assert_risk_rejected_before_broker_calls(exchange, client, order)


def test_required_risk_limits_reject_second_order_before_second_broker_call(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    first = _ziplime_order(
        asset,
        order_id="first-order",
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )
    second = _ziplime_order(
        asset,
        order_id="second-order",
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )

    asyncio.run(exchange.submit_order(first))
    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(exchange.submit_order(second))

    assert len(client.trading.validated_orders) == 1
    assert len(client.trading.placed_orders) == 1


@pytest.mark.parametrize(
    "missing_name",
    tuple(name for name in ORDER_RISK_ENV if name != "ZIPLIME_ALLOWED_SYMBOLS"),
)
def test_required_risk_limits_fail_closed_when_configuration_is_missing(
    monkeypatch, required_order_risk_limits, missing_name
):
    monkeypatch.delenv(missing_name)
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    order = _ziplime_order(
        asset,
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )

    with pytest.raises(RuntimeError):
        exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
        asyncio.run(exchange.submit_order(order))

    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []


@pytest.mark.parametrize(
    ("name", "malformed_value"),
    [
        ("ZIPLIME_ALLOWED_SYMBOLS", " , "),
        ("ZIPLIME_ALLOW_SELL", "sometimes"),
        ("ZIPLIME_MAX_ORDER_QUANTITY", "not-a-number"),
        ("ZIPLIME_MAX_ORDER_NOTIONAL_USD", "NaN"),
        ("ZIPLIME_MAX_ORDERS_PER_INVOCATION", "1.5"),
    ],
)
def test_required_risk_limits_fail_closed_when_configuration_is_malformed(
    monkeypatch, required_order_risk_limits, name, malformed_value
):
    monkeypatch.setenv(name, malformed_value)
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    order = _ziplime_order(
        asset,
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )

    with pytest.raises(RuntimeError):
        exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
        asyncio.run(exchange.submit_order(order))

    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []


@pytest.mark.parametrize(
    ("lime_status", "expected"),
    [
        (LimeOrderStatus.NEW, OrderStatus.OPEN),
        (LimeOrderStatus.PARTIALLY_FILLED, OrderStatus.OPEN),
        (LimeOrderStatus.FILLED, OrderStatus.FILLED),
        (LimeOrderStatus.CANCELED, OrderStatus.CANCELLED),
        (LimeOrderStatus.REJECTED, OrderStatus.REJECTED),
        (LimeOrderStatus.SUSPENDED, OrderStatus.HELD),
    ],
)
def test_status_mapping(lime_status, expected):
    assert LimeTraderSdkExchange._ziplime_status(lime_status) == expected


def test_sell_order_details_map_quantity_and_fill_with_negative_sign():
    asset = _asset()
    exchange = _exchange(FakeAsyncLimeClient(), FakeAssetService(asset))
    details = _details(
        side=OrderSide.SELL,
        status=LimeOrderStatus.PARTIALLY_FILLED,
        quantity="10",
        executed_quantity="3",
        order_type=OrderType.LIMIT,
        price="188.10",
        executed_timestamp=_dt(15, 30),
    )

    order = asyncio.run(exchange._order_from_sdk(details))

    assert order.amount == -10
    assert order.filled == -3
    assert order.status == OrderStatus.OPEN
    assert order.exchange_order_id == "broker-order-1"
    assert order.trading_account_id == ACCOUNT_NUMBER
    assert isinstance(order.execution_style, LimitOrder)
    assert order.limit == 188.10


def test_cancel_accepts_client_id_and_updates_local_status():
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        order_id="client-order-1",
        exchange_order_id="broker-order-1",
    )
    exchange._orders_by_external_id["broker-order-1"] = order

    asyncio.run(exchange.cancel_order("client-order-1"))

    assert client.trading.cancel_calls == [
        {"order_id": "broker-order-1", "message": "Cancelled by ziplime"}
    ]
    assert order.status == OrderStatus.CANCELLED


def test_risk_controlled_cancel_rejects_expired_deadline_without_broker_call(
    monkeypatch, required_order_risk_limits
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        order_id="submitted-client-order",
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )
    asyncio.run(exchange.submit_order(order))
    monkeypatch.setenv("ZIPLIME_ORDER_DEADLINE_UTC", "2000-01-01T00:00:00+00:00")

    with pytest.raises(RuntimeError, match="order deadline expired"):
        asyncio.run(exchange.cancel_order(order.id))

    assert client.trading.cancel_calls == []


@pytest.mark.parametrize("known_but_unsubmitted", [False, True])
def test_risk_controlled_cancel_rejects_unknown_or_unsubmitted_order(
    required_order_risk_limits, known_but_unsubmitted
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    target = "unknown-broker-order"
    if known_but_unsubmitted:
        target = "discovered-broker-order"
        exchange._orders_by_external_id[target] = _ziplime_order(
            asset,
            order_id="discovered-client-order",
            amount=1,
            style=LimitOrder(limit_price=300.0),
            exchange_order_id=target,
        )

    with pytest.raises(ValueError, match="submitted by this invocation"):
        asyncio.run(exchange.cancel_order(target))

    assert client.trading.cancel_calls == []


def test_risk_controlled_cancel_allows_same_invocation_submitted_order(
    required_order_risk_limits,
):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        order_id="same-invocation-client-order",
        amount=1,
        style=LimitOrder(limit_price=300.0),
    )
    asyncio.run(exchange.submit_order(order))

    asyncio.run(exchange.cancel_order(order.id))

    assert client.trading.cancel_calls == [
        {"order_id": "broker-order-1", "message": "Cancelled by ziplime"}
    ]
    assert order.status == OrderStatus.CANCELLED


def test_transactions_are_signed_closed_and_deduplicated_by_trade_id():
    asset = _asset()
    trade_time = _dt(15, 30)
    trade = AccountTrade(
        symbol="AAPL",
        timestamp=trade_time,
        quantity=5,
        price=Decimal("151.75"),
        amount=Decimal("758.75"),
        side=TradeSide.BUY,
        trade_id="trade-1",
    )
    client = FakeAsyncLimeClient(trades=[trade])
    details = _details(
        status=LimeOrderStatus.FILLED,
        quantity="5",
        executed_quantity="5",
        executed_timestamp=trade_time,
    )
    client.trading.details_by_id[details.order_id] = details
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        order_id="client1",
        amount=5,
        exchange_order_id=details.order_id,
    )
    exchange._orders_by_external_id[details.order_id] = order
    orders = {asset: {order.id: order}}

    first_transactions, commissions, first_closed = asyncio.run(
        exchange.get_transactions(
            orders=orders,
            current_dt=_dt(16, 0),
            same_bar_execution=False,
        )
    )
    second_transactions, _, second_closed = asyncio.run(
        exchange.get_transactions(
            orders=orders,
            current_dt=_dt(16, 1),
            same_bar_execution=False,
        )
    )

    assert commissions == []
    assert len(first_transactions) == 1
    transaction = first_transactions[0]
    assert transaction.id == "trade-1"
    assert transaction.order_id == "client1"
    assert transaction.asset == asset
    assert transaction.amount == 5
    assert transaction.price == 151.75
    assert transaction.trading_account_id == ACCOUNT_NUMBER
    assert first_closed == [order]
    assert second_transactions == []
    assert second_closed == [order]
    assert exchange._reported_filled_quantity[details.order_id] == 5
    assert exchange._processed_trade_ids == {"trade-1"}


def test_terminal_partially_filled_order_waits_for_every_account_trade_before_closing():
    asset = _asset()
    first_trade = AccountTrade(
        symbol="AAPL",
        timestamp=_dt(15, 30),
        quantity=3,
        price=Decimal("151.00"),
        amount=Decimal("453.00"),
        side=TradeSide.BUY,
        trade_id="partial-trade-1",
    )
    final_trade = AccountTrade(
        symbol="AAPL",
        timestamp=_dt(15, 31),
        quantity=2,
        price=Decimal("151.25"),
        amount=Decimal("302.50"),
        side=TradeSide.BUY,
        trade_id="partial-trade-2",
    )
    client = FakeAsyncLimeClient(trades=[first_trade])
    details = _details(
        status=LimeOrderStatus.CANCELED,
        quantity="10",
        executed_quantity="5",
        executed_timestamp=final_trade.timestamp,
    )
    client.trading.details_by_id[details.order_id] = details
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(
        asset,
        order_id="client1",
        amount=10,
        exchange_order_id=details.order_id,
    )
    exchange._orders_by_external_id[details.order_id] = order
    orders = {asset: {order.id: order}}

    first_transactions, _, first_closed = asyncio.run(
        exchange.get_transactions(
            orders=orders,
            current_dt=_dt(16, 0),
            same_bar_execution=False,
        )
    )

    assert [transaction.amount for transaction in first_transactions] == [3]
    assert first_closed == []
    assert exchange._reported_filled_quantity[details.order_id] == 3

    client.account.trades.append(final_trade)
    final_transactions, _, final_closed = asyncio.run(
        exchange.get_transactions(
            orders=orders,
            current_dt=_dt(16, 1),
            same_bar_execution=False,
        )
    )

    assert [transaction.amount for transaction in final_transactions] == [2]
    assert final_closed == [order]
    assert exchange._reported_filled_quantity[details.order_id] == 5
    assert exchange._processed_trade_ids == {"partial-trade-1", "partial-trade-2"}


def test_order_and_transaction_lookups_accept_broker_and_ziplime_client_ids():
    asset = _asset()
    broker_id = "broker-order-1"
    ziplime_id = "ziplime-client-1"

    for lookup_id in (broker_id, ziplime_id):
        trade = AccountTrade(
            symbol="AAPL",
            timestamp=_dt(15, 30),
            quantity=5,
            price=Decimal("151.75"),
            amount=Decimal("758.75"),
            side=TradeSide.BUY,
            trade_id=f"trade-for-{lookup_id}",
        )
        client = FakeAsyncLimeClient(trades=[trade])
        details = _details(
            order_id=broker_id,
            client_order_id=ziplime_id,
            status=LimeOrderStatus.FILLED,
            quantity="5",
            executed_quantity="5",
            executed_timestamp=trade.timestamp,
        )
        client.trading.details_by_id[broker_id] = details
        exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
        order = _ziplime_order(
            asset,
            order_id=ziplime_id,
            amount=5,
            exchange_order_id=broker_id,
        )
        exchange._orders_by_external_id[broker_id] = order

        found_orders = asyncio.run(exchange.get_orders_by_ids([lookup_id]))
        transactions = asyncio.run(exchange.get_transactions_by_order_ids([lookup_id]))

        assert found_orders == [order]
        assert client.trading.details_calls == [broker_id, broker_id]
        assert len(transactions) == 1
        assert transactions[0].order_id == ziplime_id
        assert transactions[0].amount == 5


def test_current_quote_uses_last_trade_for_close_and_price():
    asset = _asset()
    quote_time = _dt(19, 59)
    client = FakeAsyncLimeClient()
    client.market.quotes = [
        Quote(
            symbol="AAPL",
            ask=Decimal("191.20"),
            ask_size=Decimal("12"),
            bid=Decimal("191.10"),
            bid_size=Decimal("10"),
            last=Decimal("191.15"),
            last_size=Decimal("2"),
            volume=1_000_000,
            date=quote_time,
            high=Decimal("192.00"),
            low=Decimal("188.00"),
            open=Decimal("189.00"),
            close=Decimal("187.50"),
            week52_high=Decimal("220.00"),
            week52_low=Decimal("150.00"),
            change=Decimal("3.65"),
            change_pc=Decimal("1.95"),
        )
    ]
    exchange = _exchange(client, FakeAssetService(asset))

    frame = asyncio.run(
        exchange.get_spot_value(
            assets=frozenset({asset}),
            fields=frozenset({"close", "price", "ask", "bid", "volume"}),
            dt=_dt(20, 0),
        )
    )

    row = frame.to_dicts()[0]
    assert row["symbol"] == "AAPL"
    assert row["sid"] == 1
    assert row["close"] == 191.15
    assert row["price"] == 191.15
    assert row["ask"] == 191.20
    assert row["bid"] == 191.10
    assert row["volume"] == 1_000_000.0
    assert exchange.get_last_traded_dt(asset) == quote_time
    assert client.market.current_quote_calls == [["AAPL"]]


def test_history_maps_sdk_period_and_applies_limit_and_end_exclusion():
    asset = _asset()
    client = FakeAsyncLimeClient()
    client.market.history_by_symbol["AAPL"] = [
        QuoteHistory(
            timestamp=_dt(14, minute),
            period=Period.MINUTE,
            open=Decimal(str(100 + minute)),
            high=Decimal(str(101 + minute)),
            low=Decimal(str(99 + minute)),
            close=Decimal(str(100.5 + minute)),
            volume=1_000 + minute,
        )
        for minute in (0, 1, 2)
    ]
    exchange = _exchange(client, FakeAssetService(asset))

    frame = asyncio.run(
        exchange.get_data_by_limit(
            fields=frozenset({"open", "close", "price", "volume"}),
            limit=2,
            end_date=_dt(14, 2),
            frequency="1m",
            assets=frozenset({asset}),
            include_end_date=False,
        )
    )

    assert frame["date"].to_list() == [_dt(14, 0), _dt(14, 1)]
    assert frame["close"].to_list() == [100.5, 101.5]
    assert frame["price"].to_list() == [100.5, 101.5]
    assert frame["volume"].to_list() == [1000.0, 1001.0]
    assert client.market.history_calls[0]["symbol"] == "AAPL"
    assert client.market.history_calls[0]["period"] == Period.MINUTE
    assert client.market.history_calls[0]["to_date"] == _dt(14, 2)


def test_history_caps_future_end_before_calling_sdk(monkeypatch):
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset))
    requested_end = _dt(20, 0) + datetime.timedelta(days=30)
    deterministic_now = _dt(20, 0)
    cap_inputs = []

    def deterministic_cap(value):
        cap_inputs.append(value)
        return deterministic_now

    monkeypatch.setattr(exchange, "_cap_history_end", deterministic_cap)

    asyncio.run(
        exchange.get_data(
            assets=[asset],
            frequency="1m",
            date_from=_dt(19, 0),
            date_to=requested_end,
        )
    )

    assert cap_inputs == [requested_end]
    assert len(client.market.history_calls) == 1
    assert client.market.history_calls[0]["from_date"] == _dt(19, 0)
    assert client.market.history_calls[0]["to_date"] == deterministic_now


def test_minute_history_splits_long_range_and_deduplicates_shared_boundary():
    asset = _asset()
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset))
    date_from = datetime.datetime(2024, 1, 2, 14, 0, tzinfo=UTC)
    date_to = date_from + datetime.timedelta(days=10)
    boundary = date_from + datetime.timedelta(days=6, hours=23)

    def bar(timestamp: datetime.datetime, close: str) -> QuoteHistory:
        close_value = Decimal(close)
        return QuoteHistory(
            timestamp=timestamp,
            period=Period.MINUTE,
            open=close_value - Decimal("0.25"),
            high=close_value + Decimal("0.50"),
            low=close_value - Decimal("0.50"),
            close=close_value,
            volume=1_000,
        )

    first_window = ("AAPL", date_from, boundary)
    second_window = ("AAPL", boundary, date_to)
    client.market.history_by_window[first_window] = [
        bar(boundary - datetime.timedelta(minutes=1), "100.00"),
        bar(boundary, "101.00"),
    ]
    client.market.history_by_window[second_window] = [
        # Lime can include both endpoints, so the shared timestamp is expected
        # in both chunks.  The later chunk is the authoritative duplicate.
        bar(boundary, "101.25"),
        bar(boundary + datetime.timedelta(minutes=1), "102.00"),
    ]

    frame = asyncio.run(
        exchange.get_data(
            assets=[asset],
            frequency="1m",
            date_from=date_from,
            date_to=date_to,
        )
    )

    assert len(client.market.history_calls) == 2
    first_call, second_call = client.market.history_calls
    assert first_call["period"] == second_call["period"] == Period.MINUTE
    assert first_call["from_date"] == date_from
    assert first_call["to_date"] == second_call["from_date"] == boundary
    assert second_call["to_date"] == date_to
    assert frame["date"].to_list() == [
        boundary - datetime.timedelta(minutes=1),
        boundary,
        boundary + datetime.timedelta(minutes=1),
    ]
    assert frame["close"].to_list() == [100.0, 101.25, 102.0]


def test_a_refused_order_is_recorded_for_the_tick_report(required_order_risk_limits):
    """ziplime drops the exception, so the adapter has to keep the reason.

    Without this the tick is indistinguishable from a strategy that simply
    chose not to trade, and gets reported as a successful no-op.
    """
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=99, style=LimitOrder(limit_price=300.0))

    with pytest.raises((RuntimeError, ValueError)):
        asyncio.run(exchange.submit_order(order))

    assert len(exchange.order_failures) == 1
    assert "AAPL" in exchange.order_failures[0]
    assert "quantity" in exchange.order_failures[0]


def test_a_placed_order_records_no_failure(required_order_risk_limits):
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=1, style=LimitOrder(limit_price=300.0))

    asyncio.run(exchange.submit_order(order))

    assert exchange.order_failures == []


def test_no_limits_lets_an_unbounded_order_through(monkeypatch):
    """With the switch off nothing here bounds size, notional or side."""
    monkeypatch.setenv("ZIPLIME_REQUIRE_ORDER_RISK_LIMITS", "false")
    asset = _asset("NVDA")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    # 436 shares of a ~$229 stock: far past the 1-share / $500 profile.
    order = _ziplime_order(asset, amount=436, style=MarketOrder())

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert client.trading.placed_orders[0].quantity == Decimal("436")
    assert exchange.order_failures == []


# ---------------------------------------------------------------------------
# Capital allocation: the strategy sees the account and trades its allocation
# ---------------------------------------------------------------------------


def _allocated_exchange(client, asset_service, *, capital):
    calendar = get_calendar("NYSE")
    return LimeTraderSdkExchange(
        name="LIME",
        canonical_name="Lime Trading",
        country_code="US",
        clock=SimpleNamespace(trading_calendar=calendar),
        trading_calendar=calendar,
        start_cash_balance=100_000.0,
        asset_service=asset_service,
        is_default=True,
        account_id=ACCOUNT_NUMBER,
        default_mic="XNGS",
        client=client,
        capital_limit=capital,
    )


def test_the_portfolio_is_the_account_with_the_allocations_cash():
    """10 AAPL and 5 MSFT at 140 on the account, $10,000 allocated."""
    aapl, msft = _asset("AAPL", sid=1), _asset("MSFT", sid=2)
    client = FakeAsyncLimeClient(
        positions=[_position("AAPL", 10, "150.25"), _position("MSFT", 5, "400")]
    )
    exchange = _allocated_exchange(client, FakeAssetService(aapl, msft), capital=10_000.0)

    portfolio = asyncio.run(exchange.get_portfolio())

    assert set(portfolio.positions) == {
        ("LIME", ACCOUNT_NUMBER, aapl), ("LIME", ACCOUNT_NUMBER, msft)}
    assert portfolio.cash == pytest.approx(10_000 - 15 * 140.0)
    assert portfolio.starting_cash == portfolio.cash
    assert portfolio.portfolio_value == pytest.approx(
        10_000 - 15 * 140.0 + 10 * 150.25 + 5 * 400)
    assert set(asyncio.run(exchange.get_positions())) == {aapl, msft}


def test_an_overbought_position_leaves_negative_cash_so_it_sells_down():
    """2026-09-23: $50,000 allocated, 443 NVDA held off the whole account."""
    nvda = _asset("NVDA")
    client = FakeAsyncLimeClient(positions=[_position("NVDA", 443, "228.87")])
    exchange = _allocated_exchange(client, FakeAssetService(nvda), capital=50_000.0)

    portfolio = asyncio.run(exchange.get_portfolio())

    assert portfolio.cash == pytest.approx(50_000 - 443 * 140.0)
    assert portfolio.cash < 0
    assert portfolio.portfolio_value == pytest.approx(
        50_000 - 443 * 140.0 + 443 * 228.87
    )


def test_without_an_allocation_the_account_is_unchanged():
    asset = _asset()
    client = FakeAsyncLimeClient(positions=[_position()])
    exchange = _allocated_exchange(client, FakeAssetService(asset), capital=None)

    portfolio = asyncio.run(exchange.get_portfolio())

    assert portfolio.cash == 10_000.0  # broker cash
    assert portfolio.portfolio_value == 12_500.0


def test_a_holding_the_asset_database_lacks_is_left_out_not_fatal():
    # The account may hold what the instrument database cannot model; failing on
    # it failed every tick, and the strategy could not have traded it anyway.
    aapl = _asset("AAPL", sid=1)
    client = FakeAsyncLimeClient(positions=[_position("AAPL", 10), _position("ZZZZ", 3)])
    exchange = _allocated_exchange(client, FakeAssetService(aapl), capital=10_000.0)

    portfolio = asyncio.run(exchange.get_portfolio())

    assert set(portfolio.positions) == {("LIME", ACCOUNT_NUMBER, aapl)}
    assert portfolio.cash == pytest.approx(10_000 - 10 * 140.0)


def test_lime_is_a_live_venue_that_never_declares_a_delisting():
    exchange = _allocated_exchange(FakeAsyncLimeClient(), FakeAssetService(), capital=None)
    exchange.data_source = SimpleNamespace(
        last_available_bar=lambda sid=None: datetime.datetime(2026, 9, 16))

    assert exchange.live is True
    assert exchange.last_available_bar(1) is None


# ---------------------------------------------------------------------------
# Below one lot: a warning, not a failure
# ---------------------------------------------------------------------------


def test_a_zero_quantity_order_is_skipped_without_raising():
    """2026-09-23: three at-target ticks raised this and halted the deployment."""
    asset = _asset("NVDA")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=0, style=MarketOrder())

    result = asyncio.run(exchange.submit_order(order))  # must not raise

    assert result is order
    # ziplime reports any zero-open-amount order as FILLED, so read what reject()
    # actually set; what matters is that it no longer counts as open.
    assert order._status == OrderStatus.REJECTED
    assert "below one lot" in order.reason
    assert not order.open
    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []
    assert exchange.order_failures == []  # not a failure
    assert len(exchange.skipped_orders) == 1
    assert exchange.skipped_orders[0]["symbol"] == "NVDA"
    assert "below one lot" in exchange.skipped_orders[0]["reason"]


def test_a_fractional_order_is_rounded_toward_zero_and_sent():
    asset = _asset("AAPL")
    client = FakeAsyncLimeClient()
    exchange = _exchange(client, FakeAssetService(asset), account_id=ACCOUNT_NUMBER)
    order = _ziplime_order(asset, amount=2.7, style=LimitOrder(limit_price=100.0))

    asyncio.run(exchange.submit_order(order))

    assert client.trading.placed_orders[0].quantity == Decimal("2")
    assert order.amount == 2
    assert exchange.skipped_orders == []
