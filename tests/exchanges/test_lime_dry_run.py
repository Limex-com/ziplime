"""Dry run for the Lime Trader SDK exchange (issue #5).

A dry run must be indistinguishable from a live run up to the moment an order
would leave the process: same rounding, same risk limits, same deadline, same
broker-side validation. Then it writes a line instead of calling the broker,
and from that point the broker must never be asked about the order.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("lime_trader", reason="lime-trader-sdk must be installed")

from lime_trader.models.trading import ValidateOrderResponse  # noqa: E402

from ziplime.finance.domain.order_status import OrderStatus  # noqa: E402
from ziplime.finance.execution import LimitOrder, MarketOrder  # noqa: E402
from ziplime.utils.calendar_utils import get_calendar  # noqa: E402
from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import (  # noqa: E402
    LimeTraderSdkExchange,
)

from tests.exchanges import test_lime_trader_sdk_exchange as lime_tests  # noqa: E402
from tests.exchanges.test_lime_trader_sdk_exchange import (  # noqa: E402
    ACCOUNT_NUMBER,
    FakeAssetService,
    FakeAsyncLimeClient,
    _asset,
    _dt,
    _ziplime_order,
)

# Reuse the live exchange's fixtures: the autouse one keeps the process-level
# order guards unset, the other turns the fail-closed risk profile on.
_clear_aws_order_guards = lime_tests._clear_aws_order_guards
required_order_risk_limits = lime_tests.required_order_risk_limits


def _dry_exchange(client, asset_service, path, *, validate_orders=True):
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
        validate_orders=validate_orders,
        client=client,
        dry_run_file=path,
    )


def _lines(path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_a_live_exchange_is_not_a_dry_run():
    client = FakeAsyncLimeClient()
    calendar = get_calendar("NYSE")
    exchange = LimeTraderSdkExchange(
        name="LIME", canonical_name="Lime Trading", country_code="US",
        clock=SimpleNamespace(trading_calendar=calendar), trading_calendar=calendar,
        start_cash_balance=1.0, asset_service=FakeAssetService(), is_default=True,
        account_id=ACCOUNT_NUMBER, client=client,
    )
    assert exchange.dry_run is False
    assert exchange.dry_run_file is None


def test_the_file_directory_is_created_at_construction(tmp_path):
    path = tmp_path / "nested" / "orders.jsonl"
    exchange = _dry_exchange(FakeAsyncLimeClient(), FakeAssetService(), path)
    assert exchange.dry_run is True
    assert path.parent.is_dir()
    assert not path.exists(), "no file until something is recorded"


@pytest.mark.parametrize(
    "amount, style, side, order_type, limit_price",
    [
        (5, MarketOrder(), "buy", "market", None),
        (-3, LimitOrder(limit_price=187.5), "sell", "limit", "187.5"),
    ],
)
def test_an_order_is_validated_then_written_never_placed(
    tmp_path, amount, style, side, order_type, limit_price
):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)
    order = _ziplime_order(asset, order_id="strategy-order-1", amount=amount, style=style)

    result = asyncio.run(exchange.submit_order(order))

    assert result is order
    assert len(client.trading.validated_orders) == 1, "broker validation still runs"
    assert client.trading.placed_orders == []
    assert order.exchange_order_id.startswith(LimeTraderSdkExchange.DRY_RUN_ID_PREFIX)
    assert order.status == OrderStatus.OPEN

    [record] = _lines(path)
    validated = client.trading.validated_orders[0]
    assert record["event"] == "place"
    assert record["dry_run_id"] == order.exchange_order_id
    assert record["client_order_id"] == validated.client_order_id
    assert record["symbol"] == "AAPL"
    assert record["side"] == side
    assert record["quantity"] == str(abs(amount))
    assert record["order_type"] == order_type
    assert record["limit_price"] == limit_price
    assert record["time_in_force"] == "day"
    assert record["broker_validated"] is True
    assert record["ziplime_order_id"] == "strategy-order-1"
    assert record["order_dt"] == _dt(14, 0).isoformat()
    assert record["account"] == "de***nt", "account ids are masked as in the logs"


def test_without_validation_nothing_reaches_the_broker_at_all(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path, validate_orders=False)

    asyncio.run(exchange.submit_order(_ziplime_order(asset)))

    assert client.trading.validated_orders == []
    assert client.trading.placed_orders == []
    assert _lines(path)[0]["broker_validated"] is False


def test_a_broker_validation_failure_is_recorded_as_a_refusal(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    client.trading.validation_response = ValidateOrderResponse(
        is_valid=False, validation_message="insufficient buying power"
    )
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)

    with pytest.raises(ValueError, match="insufficient buying power"):
        asyncio.run(exchange.submit_order(_ziplime_order(asset)))

    [record] = _lines(path)
    assert record["event"] == "refuse"
    assert "insufficient buying power" in record["reason"]
    assert exchange.order_failures, "the tick report still sees the failure"
    assert client.trading.placed_orders == []


@pytest.mark.usefixtures("required_order_risk_limits")
def test_risk_limits_refuse_exactly_as_they_would_live(tmp_path):
    asset = _asset("TSLA", sid=2)
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)

    with pytest.raises(ValueError, match="allowlist"):
        asyncio.run(exchange.submit_order(_ziplime_order(asset, amount=1)))

    [record] = _lines(path)
    assert record["event"] == "refuse"
    assert record["symbol"] == "TSLA"
    assert client.trading.validated_orders == [], "refused before the broker, as live"


def test_a_below_one_lot_order_is_recorded_as_a_skip(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)

    asyncio.run(exchange.submit_order(_ziplime_order(asset, amount=0.4)))

    [record] = _lines(path)
    assert record["event"] == "skip"
    assert record["requested_amount"] == 0.4
    assert "below one lot" in record["reason"]


def test_the_broker_is_never_asked_about_a_dry_run_order(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)
    order = _ziplime_order(asset, order_id="strategy-order-1")
    asyncio.run(exchange.submit_order(order))

    transactions, _, closed = asyncio.run(
        exchange.get_transactions(
            orders={asset: {order.id: order}},
            current_dt=_dt(15, 0),
            same_bar_execution=False,
        )
    )
    by_id = asyncio.run(exchange.get_orders_by_ids([order.id, order.exchange_order_id]))
    open_orders = asyncio.run(exchange.get_orders())

    assert transactions == [] and closed == [], "a dry-run order never fills"
    assert client.trading.details_calls == []
    assert by_id == [order]
    assert open_orders == {order.id: order}


def test_cancel_is_recorded_and_applied_locally_only(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)
    order = _ziplime_order(asset, order_id="strategy-order-1")
    asyncio.run(exchange.submit_order(order))

    asyncio.run(exchange.cancel_order(order.id))

    assert client.trading.cancel_calls == []
    assert order.status == OrderStatus.CANCELLED
    place, cancel = _lines(path)
    assert cancel["event"] == "cancel"
    assert cancel["dry_run_id"] == place["dry_run_id"]
    assert cancel["known_order"] is True
    assert asyncio.run(exchange.get_orders()) == {}


def test_cancelling_a_real_broker_order_in_a_dry_run_does_not_reach_the_broker(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)

    asyncio.run(exchange.cancel_order("some-live-broker-order"))

    assert client.trading.cancel_calls == []
    [record] = _lines(path)
    assert record == {**record, "event": "cancel", "known_order": False}


def test_every_event_is_one_complete_json_line(tmp_path):
    asset = _asset()
    client = FakeAsyncLimeClient()
    path = tmp_path / "orders.jsonl"
    exchange = _dry_exchange(client, FakeAssetService(asset), path)
    for index in range(3):
        asyncio.run(exchange.submit_order(_ziplime_order(asset, order_id=f"o{index}")))

    text = path.read_text()
    assert text.endswith("\n")
    records = _lines(path)
    assert [r["ziplime_order_id"] for r in records] == ["o0", "o1", "o2"]
    assert len({r["dry_run_id"] for r in records}) == 3
