"""Lime Trader SDK exchange for Ziplime live trading.

It uses only :class:`lime_trader.AsyncLimeClient`, so broker I/O never blocks
Ziplime's event loop.

Only equities and the order types exposed by lime-trader-sdk 0.4.x (market and
limit) are supported.  Order submission always runs the broker-side validation
endpoint first; validation does not place an order.
"""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import math
import os
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Type

import pandas as pd
import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar
from lime_trader import AsyncLimeClient
from lime_trader.models.accounts import (
    AccountDetails,
    AccountPosition,
    AccountTrade,
    TradeSide,
)
from lime_trader.models.market import Period as LimePeriod
from lime_trader.models.page import PageRequest
from lime_trader.models.trading import (
    Order as LimeOrder,
    OrderDetails as LimeOrderDetails,
    OrderSide as LimeOrderSide,
    OrderStatus as LimeOrderStatus,
    OrderType as LimeOrderType,
    TimeInForce,
)

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.finance.domain.position import Position
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission import CommissionModel, NoCommission
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.execution import LimitOrder, MarketOrder
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.gens.domain.trading_clock import TradingClock

from ziplime.exchanges.allocation import Holding, allocated_cash, round_to_lots


_DEFAULT_US_MICS = ("XNGS", "XNYS", "XNMS", "ARCX", "BATS", "_BNCC")
_DEFAULT_FIELDS = frozenset({"open", "high", "low", "close", "price", "volume"})
_TERMINAL_STATUSES = {
    LimeOrderStatus.FILLED,
    LimeOrderStatus.CANCELED,
    LimeOrderStatus.REJECTED,
    LimeOrderStatus.REPLACED,
    LimeOrderStatus.DONE_FOR_DAY,
}


class LimeTraderSdkExchange(Exchange):
    """Ziplime exchange backed by the Lime Trader REST SDK.

    Opt-in order guards, read from the environment (all unset by default):

    * ``ZIPLIME_ORDER_DEADLINE_UTC`` -- refuse to submit or cancel after this
      instant, so a signal that went stale in a queue is never sent;
    * ``ZIPLIME_REQUIRE_ORDER_RISK_LIMITS=true`` with ``ZIPLIME_ALLOW_SELL``,
      ``ZIPLIME_MAX_ORDER_QUANTITY``, ``ZIPLIME_MAX_ORDER_NOTIONAL_USD``,
      ``ZIPLIME_MAX_ORDERS_PER_INVOCATION`` and optionally
      ``ZIPLIME_ALLOWED_SYMBOLS`` -- a fail-closed per-invocation risk profile.
    """

    live = True

    # Lime trades whole shares through this adapter; fractional quantities are
    # rounded toward zero before they are sent.
    LOT_SIZE = 1

    @staticmethod
    def _masked_account_id(value: str) -> str:
        if len(value) <= 4:
            return "****"
        return f"{value[:2]}***{value[-2:]}"

    def __init__(
        self,
        *,
        name: str,
        canonical_name: str,
        country_code: str,
        clock: TradingClock,
        trading_calendar: ExchangeCalendar,
        start_cash_balance: float,
        asset_service: AssetService,
        is_default: bool,
        account_id: str | None = None,
        lime_sdk_credentials_file: str | None = None,
        data_source: DataBundle | None = None,
        symbol_mics: dict[str, str] | None = None,
        default_mic: str = "XNGS",
        order_route: str = "auto",
        time_in_force: TimeInForce | str = TimeInForce.DAY,
        validate_orders: bool = True,
        commission_models: dict[Type[Asset], CommissionModel] | None = None,
        client: AsyncLimeClient | None = None,
        logger: Any = None,
        capital_limit: float | None = None,
    ):
        super().__init__(
            name=name,
            canonical_name=canonical_name,
            country_code=country_code,
            clock=clock,
            trading_calendar=trading_calendar,
            account_id=account_id or "",
            is_default=is_default,
            data_source=data_source,
        )

        self._logger = logger or structlog.get_logger(__name__)
        # The SDK's DEBUG HTTP logs include the username and client id.  Keep
        # its transport logger at INFO even when the application uses structlog
        # debugging; broker request bodies must not leak into container logs.
        self._sdk_logger = logging.getLogger(f"{__name__}.sdk")
        self._sdk_logger.setLevel(logging.INFO)
        self._start_cash_balance = float(start_cash_balance)
        self._asset_service = asset_service
        self._credentials_file = lime_sdk_credentials_file
        self._default_mic = default_mic
        self._symbol_mics = dict(symbol_mics or {})
        self._order_route = order_route
        self._time_in_force = (
            time_in_force
            if isinstance(time_in_force, TimeInForce)
            else TimeInForce(str(time_in_force).lower())
        )
        self._validate_orders = validate_orders
        self.commission_models = dict(commission_models or {})
        # With an allocation the strategy sees the account's positions but trades
        # its allocated capital, not the account's cash (ziplime.exchanges.allocation).
        # None trades the account as it is.
        self._capital_limit = capital_limit

        # Lambda live trading enables these process-level controls before the
        # strategy code starts.  Snapshot them at exchange construction so an
        # accidental environment mutation inside a strategy cannot relax a
        # limit midway through the invocation.
        self._order_risk_limits = self._load_order_risk_limits()
        self._order_attempt_count = 0

        if client is not None:
            self._client = client
        elif lime_sdk_credentials_file:
            credential_path = Path(lime_sdk_credentials_file)
            if not credential_path.is_file():
                raise ValueError(
                    f"Lime SDK credentials file does not exist: {credential_path}"
                )
            self._client = AsyncLimeClient.from_file(
                str(credential_path), logger=self._sdk_logger
            )
        else:
            self._client = AsyncLimeClient.from_env(logger=self._sdk_logger)

        self._last_balance: AccountDetails | None = None
        self._last_api_call_ok: bool | None = None
        self._assets_by_symbol: dict[str, ExchangeAsset] = {}
        self._last_traded_by_symbol: dict[str, datetime.datetime] = {}

        # External broker id -> Ziplime order, for callers reporting what a run
        # placed.
        self._orders_by_external_id: dict[str, Order] = {}
        self._submitted_order_ids: set[str] = set()
        self._sdk_order_details: dict[str, LimeOrderDetails] = {}
        self._reported_filled_quantity: dict[str, int] = defaultdict(int)
        self._processed_trade_ids: set[str] = set()
        # Orders this tick tried and failed to place. ziplime swallows the
        # exception (stop_on_error=False) and loses it, so the worker reads this
        # afterwards: an order the strategy wanted but could not place is a
        # failed tick, never a quiet no-op.
        self.order_failures: list[str] = []
        # Orders that rounded down to zero lots and were deliberately not sent.
        # A warning, not a failure: an at-target rebalance routinely yields a
        # fractional delta, and three such ticks must not halt a deployment.
        self.skipped_orders: list[dict] = []
        self._session_start_utc = datetime.datetime.now(tz=datetime.timezone.utc)

    # ------------------------------------------------------------------
    # Account and portfolio
    # ------------------------------------------------------------------
    def get_start_cash_balance(self) -> float:
        return self._start_cash_balance

    def get_current_cash_balance(self) -> float:
        if self._last_balance is None:
            return self._start_cash_balance
        return float(self._last_balance.cash)

    async def _get_account_balance(self) -> AccountDetails:
        balances = await self._client.account.get_balances()
        self._last_api_call_ok = True
        if not balances:
            raise RuntimeError(
                "Lime returned no brokerage accounts for these credentials."
            )

        if self.account_id:
            balance = next(
                (b for b in balances if b.account_number == self.account_id), None
            )
            if balance is None:
                raise ValueError(
                    f"Lime account {self.account_id!r} is not available to these credentials."
                )
        elif len(balances) == 1:
            balance = balances[0]
            self.account_id = balance.account_number
            self._logger.info(
                "Selected the only Lime account",
                account_id=self._masked_account_id(self.account_id),
            )
        else:
            raise ValueError(
                "Several Lime accounts are available. Set job-spec account_id or "
                "LIME_SDK_ACCOUNT_ID explicitly; automatic selection is disabled for safety."
            )

        self._last_balance = balance
        return balance

    async def _resolve_asset(
        self, symbol: str, preferred_mic: str | None = None
    ) -> ExchangeAsset:
        cached = self._assets_by_symbol.get(symbol)
        if cached is not None:
            return cached

        candidates = (
            preferred_mic,
            self._symbol_mics.get(symbol),
            self._default_mic,
            *_DEFAULT_US_MICS,
        )
        seen: set[str] = set()
        for mic in candidates:
            if not mic or mic in seen:
                continue
            seen.add(mic)
            asset = await self._asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=symbol, mic=mic),
                asset_type=AssetType.EQUITY,
            )
            if asset is not None:
                self._assets_by_symbol[symbol] = asset
                return asset

        raise ValueError(
            f"Lime position/order symbol {symbol!r} is absent from assets.sqlite. "
            f"Add it with its listing MIC (tried: {', '.join(seen)})."
        )

    async def _positions_from_sdk(
        self, sdk_positions: list[AccountPosition]
    ) -> dict[ExchangeAsset, Position]:
        positions: dict[ExchangeAsset, Position] = {}
        for sdk_position in sdk_positions:
            # Lime keeps a zero-quantity row for every symbol traded today.
            # ziplime's position stats size their arrays from the non-zero
            # positions but iterate over all of them, so a flat row overflows
            # the index array.
            amount = int(sdk_position.quantity)
            if amount == 0:
                continue
            try:
                asset = await self._resolve_asset(sdk_position.symbol)
            except ValueError:
                # The strategy sees the whole account, which may hold what the
                # instrument database cannot model. Failing on it failed every
                # tick; the strategy could not have traded that holding anyway.
                self._logger.warning(
                    "Lime position in an instrument the asset database lacks; "
                    "left out of the portfolio",
                    symbol=sdk_position.symbol,
                    quantity=amount,
                )
                continue
            current_price = float(sdk_position.current_price)
            positions[asset] = Position(
                asset=asset,
                exchange_name=self.name,
                trading_account_id=self.account_id,
                amount=amount,
                cost_basis=float(sdk_position.average_open_price),
                last_sale_price=current_price,
                last_sale_date=None,
            )
        return positions

    async def get_positions(self) -> dict[ExchangeAsset, Position]:
        await self._get_account_balance()
        sdk_positions = await self._client.account.get_positions(
            account_number=self.account_id,
            date=None,
            strategy=None,
        )
        self._last_api_call_ok = True
        return await self._positions_from_sdk(sdk_positions)

    async def get_portfolio(self) -> Portfolio:
        balance = await self._get_account_balance()
        sdk_positions = await self._client.account.get_positions(
            account_number=self.account_id,
            date=None,
            strategy=None,
        )
        # ziplime seeds its ledger from this, so it is what order_target_percent
        # sizes against.
        positions = await self._positions_from_sdk(sdk_positions)
        positions_value = sum(p.amount * p.last_sale_price for p in positions.values())
        positions_exposure = sum(
            abs(p.amount * p.last_sale_price) for p in positions.values()
        )

        broker_cash = float(balance.cash)
        if self._capital_limit is None:
            cash = broker_cash
            portfolio_value = float(balance.account_value_total)
        else:
            cash = allocated_cash(
                capital=self._capital_limit,
                holdings=[
                    Holding(asset.symbol, p.amount, p.cost_basis)
                    for asset, p in positions.items()
                ],
            )
            # Allocation plus the unrealised PnL of its own positions.
            portfolio_value = cash + positions_value
            self._logger.info(
                "Capital allocation applied",
                capital=self._capital_limit,
                broker_cash=broker_cash,
                strategy_cash=cash,
                strategy_portfolio_value=portfolio_value,
                positions={a.symbol: p.amount for a, p in positions.items()},
            )

        return Portfolio(
            start_date=datetime.datetime.now(tz=self.trading_calendar.tz),
            starting_cash=cash,
            portfolio_value=portfolio_value,
            cash=cash,
            cash_flow=0.0,
            pnl=0.0,
            returns=0.0,
            positions_value=positions_value,
            positions_exposure=positions_exposure,
            # Keyed the way the ledger reads a portfolio and order_target_* looks a
            # holding up: (exchange, trading account, asset).
            positions={
                (self.name, self.account_id, asset): position
                for asset, position in positions.items()
            },
        )

    async def get_account(self) -> Account:
        balance = await self._get_account_balance()
        net_liquidation = float(balance.account_value_total)
        net_positions = float(balance.position_market_value)
        leverage = abs(net_positions) / net_liquidation if net_liquidation else 0.0
        return Account(
            settled_cash=float(balance.cash),
            accrued_interest=0.0,
            buying_power=float(balance.margin_buying_power),
            equity_with_loan=net_liquidation,
            total_positions_value=net_positions,
            total_positions_exposure=abs(net_positions),
            regt_equity=net_liquidation,
            regt_margin=0.0,
            initial_margin_requirement=0.0,
            maintenance_margin_requirement=0.0,
            available_funds=float(balance.cash_to_withdraw),
            excess_liquidity=float(balance.non_margin_buying_power),
            cushion=(float(balance.cash) / net_liquidation if net_liquidation else 0.0),
            day_trades_remaining=max(0.0, 3.0 - float(balance.daytrades_count)),
            leverage=leverage,
            net_leverage=(net_positions / net_liquidation if net_liquidation else 0.0),
            net_liquidation=net_liquidation,
        )

    def get_time_skew(self) -> datetime.timedelta:
        return datetime.timedelta(0)

    def is_alive(self) -> bool:
        return self._last_api_call_ok is not False

    # ------------------------------------------------------------------
    # Orders and fills
    # ------------------------------------------------------------------
    @staticmethod
    def _client_order_id(order_id: str) -> str:
        """Return a stable SDK-safe id (max 32, alphanumeric or spaces)."""
        raw = str(order_id)
        if re.fullmatch(r"[A-Za-z0-9 ]{1,32}", raw):
            return raw
        readable = re.sub(r"[^A-Za-z0-9 ]", "", raw)[:20]
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
        return f"{readable}{digest}"[:32]

    @staticmethod
    def _assert_order_deadline() -> None:
        """Fail closed if an AWS-scheduled signal became stale before submit."""

        raw = os.environ.get("ZIPLIME_ORDER_DEADLINE_UTC", "").strip()
        if not raw:
            return
        try:
            deadline = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("invalid ZIPLIME_ORDER_DEADLINE_UTC") from exc
        if deadline.tzinfo is None:
            raise RuntimeError("ZIPLIME_ORDER_DEADLINE_UTC must include a timezone")
        if datetime.datetime.now(datetime.timezone.utc) > deadline.astimezone(
            datetime.timezone.utc
        ):
            raise RuntimeError("Lime order deadline expired before broker submission")

    @staticmethod
    def _load_order_risk_limits() -> dict[str, Any] | None:
        raw_required = (
            os.environ.get("ZIPLIME_REQUIRE_ORDER_RISK_LIMITS", "false").strip().lower()
        )
        if raw_required not in {"true", "false"}:
            raise RuntimeError(
                "ZIPLIME_REQUIRE_ORDER_RISK_LIMITS must be true or false"
            )
        if raw_required == "false":
            return None

        # The traded universe belongs to the strategy, not to a deploy-time pin:
        # one worker serves many deployments, so a fixed allowlist would silently
        # reject every instrument except whichever one the image was built around.
        # Unset (or "*") therefore means no symbol restriction. An explicit list
        # still restricts, for an image deliberately dedicated to one instrument.
        # The quantity, notional, order-count and no-short limits always apply.
        raw_symbols = os.environ.get("ZIPLIME_ALLOWED_SYMBOLS", "").strip()
        if raw_symbols in {"", "*"}:
            allowed_symbols = None
        else:
            allowed_symbols = frozenset(
                symbol.strip().upper()
                for symbol in raw_symbols.split(",")
                if symbol.strip()
            )
            if not allowed_symbols:
                # Separators only, e.g. " , ". Silently treating that as an empty
                # allowlist would deny every order; a malformed pin is a config
                # error and must say so.
                raise RuntimeError(
                    "ZIPLIME_ALLOWED_SYMBOLS names no symbol; use '*' to allow all"
                )
            if any(
                not re.fullmatch(r"[A-Z0-9.-]{1,32}", symbol)
                for symbol in allowed_symbols
            ):
                raise RuntimeError("ZIPLIME_ALLOWED_SYMBOLS contains an invalid symbol")

        raw_allow_sell = os.environ.get("ZIPLIME_ALLOW_SELL", "").strip().lower()
        if raw_allow_sell not in {"true", "false"}:
            raise RuntimeError(
                "ZIPLIME_ALLOW_SELL must be explicitly set to true or false"
            )
        if raw_allow_sell == "true":
            raise RuntimeError(
                "ZIPLIME_ALLOW_SELL=true is not supported by the fail-closed "
                "Lambda risk profile because a sell cannot yet be proven to avoid "
                "opening a short position"
            )

        def positive_decimal(name: str) -> Decimal:
            raw = os.environ.get(name, "").strip()
            try:
                value = Decimal(raw)
            except Exception as exc:
                raise RuntimeError(f"{name} must be a positive decimal") from exc
            if not value.is_finite() or value <= 0:
                raise RuntimeError(f"{name} must be a positive finite decimal")
            return value

        raw_max_orders = os.environ.get("ZIPLIME_MAX_ORDERS_PER_INVOCATION", "").strip()
        try:
            max_orders = int(raw_max_orders)
        except ValueError as exc:
            raise RuntimeError(
                "ZIPLIME_MAX_ORDERS_PER_INVOCATION must be a positive integer"
            ) from exc
        if max_orders <= 0 or str(max_orders) != raw_max_orders:
            raise RuntimeError(
                "ZIPLIME_MAX_ORDERS_PER_INVOCATION must be a positive integer"
            )

        return {
            "allowed_symbols": allowed_symbols,
            "allow_sell": False,
            "max_quantity": positive_decimal("ZIPLIME_MAX_ORDER_QUANTITY"),
            "max_notional": positive_decimal("ZIPLIME_MAX_ORDER_NOTIONAL_USD"),
            "max_orders": max_orders,
        }

    def _assert_order_risk_limits(self, order: Order) -> None:
        limits = self._order_risk_limits
        if limits is None:
            return

        symbol = str(order.asset.symbol).strip().upper()
        allowed = limits["allowed_symbols"]
        if allowed is not None and symbol not in allowed:
            raise ValueError(f"Lime order symbol {symbol!r} is not allowlisted")

        quantity = Decimal(str(abs(order.amount)))
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError("Lime order quantity must be positive and finite")
        if quantity > limits["max_quantity"]:
            raise ValueError("Lime order quantity exceeds the configured risk limit")
        if order.amount < 0 and not limits["allow_sell"]:
            raise ValueError(
                "Lime sell orders are disabled by the configured risk limits"
            )

        if isinstance(order.execution_style, LimitOrder):
            limit_price = Decimal(str(order.limit))
            if not limit_price.is_finite() or limit_price <= 0:
                raise ValueError("Lime limit price must be positive and finite")
            if quantity * limit_price > limits["max_notional"]:
                raise ValueError(
                    "Lime order notional exceeds the configured risk limit"
                )
        elif not isinstance(order.execution_style, MarketOrder):
            raise ValueError(
                "Lime supports MarketOrder and LimitOrder only; got "
                f"{type(order.execution_style).__name__}"
            )
        # A market order carries no price of its own, so the notional limit still
        # applies but can only be evaluated against the live quote — submit_order
        # awaits _assert_market_order_notional before the order reaches Lime.

        if self._order_attempt_count >= limits["max_orders"]:
            raise ValueError("Lime order count exceeds the per-invocation risk limit")
        self._order_attempt_count += 1

    async def _assert_market_order_notional(self, order: Order) -> None:
        """Bound a market order's exposure against the live quote.

        Market orders used to be refused outright because an unpriced order
        cannot be checked against ZIPLIME_MAX_ORDER_NOTIONAL_USD. Pricing it at
        the side of the book it will actually cross restores that cap instead of
        letting the one limit that bounds money slip past unchecked. Fails closed:
        no usable quote means no order.
        """
        limits = self._order_risk_limits
        if limits is None or not isinstance(order.execution_style, MarketOrder):
            return

        symbol = str(order.asset.symbol)
        quotes = await self._client.market.get_current_quotes(symbols=[symbol])
        quote = next(
            (q for q in quotes if str(q.symbol).upper() == symbol.upper()), None
        )
        if quote is None:
            raise ValueError(
                f"Lime returned no current quote for {symbol!r}; a market order "
                "cannot be bounded without one"
            )
        # Buys cross the ask, sells the bid: price the worst side, not the mid.
        reference = quote.ask if order.amount > 0 else quote.bid
        price = Decimal(str(reference))
        if not price.is_finite() or price <= 0:
            raise ValueError(f"Lime quote for {symbol!r} carries no usable price")
        if Decimal(str(abs(order.amount))) * price > limits["max_notional"]:
            raise ValueError(
                "Lime market order notional exceeds the configured risk limit"
            )

    @staticmethod
    def _ziplime_status(status: LimeOrderStatus) -> OrderStatus:
        if status in {
            LimeOrderStatus.NEW,
            LimeOrderStatus.PENDING_NEW,
            LimeOrderStatus.PENDING_CANCEL,
            LimeOrderStatus.PARTIALLY_FILLED,
        }:
            return OrderStatus.OPEN
        if status == LimeOrderStatus.FILLED:
            return OrderStatus.FILLED
        if status in {
            LimeOrderStatus.CANCELED,
            LimeOrderStatus.REPLACED,
            LimeOrderStatus.DONE_FOR_DAY,
        }:
            return OrderStatus.CANCELLED
        if status == LimeOrderStatus.REJECTED:
            return OrderStatus.REJECTED
        if status == LimeOrderStatus.SUSPENDED:
            return OrderStatus.HELD
        raise ValueError(f"Unsupported Lime order status: {status!r}")

    async def _order_from_sdk(self, details: LimeOrderDetails) -> Order:
        external_id = details.order_id
        original = self._orders_by_external_id.get(external_id)
        signed_quantity = int(details.quantity)
        signed_filled = int(details.executed_quantity)
        if details.order_side == LimeOrderSide.SELL:
            signed_quantity = -signed_quantity
            signed_filled = -signed_filled

        status = self._ziplime_status(details.order_status)
        if original is not None:
            original.amount = signed_quantity
            original.filled = signed_filled
            original.status = status
            original.exchange_order_id = external_id
            self._sdk_order_details[external_id] = details
            return original

        asset = await self._resolve_asset(details.symbol)
        if details.order_type == LimeOrderType.MARKET:
            style = MarketOrder()
        elif details.order_type == LimeOrderType.LIMIT:
            style = LimitOrder(limit_price=float(details.price))
        else:
            raise ValueError(f"Unsupported Lime order type: {details.order_type!r}")

        # OrderDetails has no creation timestamp.  For an order discovered
        # after this process started, use the connector session start instead
        # of the last execution timestamp.  The latter would make earlier
        # partial fills look older than the order and silently skip them.
        dt = self._session_start_utc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        order = Order(
            id=details.client_order_id or external_id,
            dt=dt.astimezone(self.trading_calendar.tz),
            asset=asset,
            amount=signed_quantity,
            filled=signed_filled,
            commission=0.0,
            execution_style=style,
            status=status,
            exchange_name=self.name,
            exchange_order_id=external_id,
            trading_account_id=details.account_number,
        )
        self._orders_by_external_id[external_id] = order
        self._sdk_order_details[external_id] = details
        return order

    async def submit_order(self, order: Order) -> Order:
        """Place one order, keeping the reason when it cannot be placed.

        ziplime runs the bar with stop_on_error=False, so it catches whatever is
        raised here and drops it. Without recording the refusal the tick looks
        exactly like a strategy that chose not to trade, and gets reported as a
        successful no-op.
        """
        quantity = round_to_lots(order.amount, self.LOT_SIZE)
        if quantity == 0:
            return self._skip_order(order)
        if quantity != order.amount:
            self._logger.info(
                "Order rounded to whole lots",
                symbol=order.asset.symbol,
                requested=order.amount,
                sent=quantity,
                lot_size=self.LOT_SIZE,
            )
            order.amount = int(quantity)
        try:
            return await self._submit_order(order)
        except Exception as exc:
            self.order_failures.append(
                f"{order.asset.symbol}: {type(exc).__name__}: {exc}"
            )
            raise

    def _skip_order(self, order: Order) -> Order:
        """Leave a below-one-lot order unsent, as a warning rather than a failure.

        An at-target rebalance routinely asks for a fraction of a share. Raising
        here aborted the whole handle_data — so orders for the other symbols were
        never sent — and failed the tick; on 2026-09-23 three such ticks in a row
        ("Lime order quantity must be non-zero") put a healthy deployment into
        `error`. Nothing is broken when the delta is below a lot.
        """
        symbol = str(order.asset.symbol)
        reason = (
            f"{abs(order.amount):g} {symbol} is below one lot "
            f"({self.LOT_SIZE:g} share); order not sent"
        )
        self._logger.warning(
            "Order below one lot skipped",
            symbol=symbol,
            requested=order.amount,
            lot_size=self.LOT_SIZE,
        )
        order.reject(reason=reason)
        self.skipped_orders.append(
            {
                "symbol": symbol,
                "requested": float(order.amount),
                "lot_size": self.LOT_SIZE,
                "reason": reason,
            }
        )
        return order

    async def _submit_order(self, order: Order) -> Order:
        if order.amount == 0:
            raise ValueError("Lime order quantity must be non-zero.")
        self._assert_order_risk_limits(order)
        await self._assert_market_order_notional(order)
        self._assets_by_symbol[order.asset.symbol] = order.asset
        await self._get_account_balance()

        if isinstance(order.execution_style, MarketOrder):
            order_type = LimeOrderType.MARKET
            price = None
        elif isinstance(order.execution_style, LimitOrder):
            order_type = LimeOrderType.LIMIT
            price = Decimal(str(order.limit))
        else:
            raise ValueError(
                "Lime Trader SDK supports MarketOrder and LimitOrder only; "
                f"got {type(order.execution_style).__name__}."
            )

        sdk_order = LimeOrder(
            account_number=self.account_id,
            symbol=order.asset.symbol,
            quantity=Decimal(abs(order.amount)),
            exchange=self._order_route,
            client_order_id=self._client_order_id(order.id),
            tag=f"ziplime {self._client_order_id(order.id)}"[:32],
            price=price,
            time_in_force=self._time_in_force,
            order_type=order_type,
            side=LimeOrderSide.BUY if order.amount > 0 else LimeOrderSide.SELL,
        )

        if self._validate_orders:
            self._assert_order_deadline()
            validation = await self._client.trading.validate_order(order=sdk_order)
            if not validation.is_valid:
                reason = (
                    validation.validation_message or "unknown broker validation error"
                )
                raise ValueError(f"Lime rejected order during validation: {reason}")

        self._assert_order_deadline()
        response = await self._client.trading.place_order(order=sdk_order)
        self._last_api_call_ok = True
        if not response.success or not response.order_id:
            raise RuntimeError("Lime did not accept the order placement request.")

        order.exchange_order_id = response.order_id
        order.trading_account_id = self.account_id
        self._orders_by_external_id[response.order_id] = order
        self._submitted_order_ids.add(response.order_id)
        self._logger.info(
            "Submitted Lime order",
            exchange_order_id=response.order_id,
            client_order_id=sdk_order.client_order_id,
            symbol=sdk_order.symbol,
            quantity=str(sdk_order.quantity),
            side=sdk_order.side.value,
            order_type=sdk_order.order_type.value,
            account_id=self._masked_account_id(self.account_id),
        )
        return order

    async def get_orders(self) -> dict[str, Order]:
        await self._get_account_balance()
        active = await self._client.trading.get_active_orders(
            account_number=self.account_id
        )
        result: dict[str, Order] = {}
        for details in active:
            order = await self._order_from_sdk(details)
            result[order.id] = order
        return result

    async def get_orders_by_ids(self, order_ids: list[str]) -> list[Order]:
        ids = []
        for order_id in dict.fromkeys(order_ids):
            if not order_id:
                continue
            if order_id in self._orders_by_external_id:
                ids.append(order_id)
                continue
            external_id = next(
                (
                    broker_id
                    for broker_id, known_order in self._orders_by_external_id.items()
                    if known_order.id == order_id
                ),
                order_id,
            )
            ids.append(external_id)
        if not ids:
            return []
        details = await asyncio.gather(
            *(
                self._client.trading.get_order_details(order_id=order_id)
                for order_id in ids
            )
        )
        return [await self._order_from_sdk(item) for item in details]

    @staticmethod
    def _as_datetime(value: datetime.datetime | int | float) -> datetime.datetime:
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=datetime.timezone.utc)
            return value
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        return datetime.datetime.fromtimestamp(numeric, tz=datetime.timezone.utc)

    async def _get_account_trades(
        self, dates: set[datetime.date]
    ) -> list[AccountTrade]:
        trades: list[AccountTrade] = []
        for trade_date in sorted(dates):
            page_number = 1
            while True:
                page = await self._client.account.get_trades(
                    account_number=self.account_id,
                    date=trade_date,
                    page=PageRequest(page=page_number, size=500),
                )
                trades.extend(page.data)
                if page.is_last():
                    break
                page_number += 1
        return trades

    async def get_transactions(
        self,
        orders: dict[ExchangeAsset, dict[str, Order]],
        current_dt: datetime.datetime,
        same_bar_execution: bool,
    ) -> tuple[list[Transaction], list[dict], list[Order]]:
        del same_bar_execution  # Broker fills, not simulated bar fills, determine execution.
        open_orders = [
            order
            for asset_orders in orders.values()
            for order in asset_orders.values()
            if order.exchange_order_id
        ]
        if not open_orders:
            return [], [], []

        details_list = await asyncio.gather(
            *(
                self._client.trading.get_order_details(order_id=order.exchange_order_id)
                for order in open_orders
            )
        )
        details_by_id = {details.order_id: details for details in details_list}
        refreshed_orders = [
            await self._order_from_sdk(details) for details in details_list
        ]

        dates = {
            order.dt.astimezone(self.trading_calendar.tz).date()
            for order in open_orders
            if order.dt is not None
        }
        dates.add(current_dt.astimezone(self.trading_calendar.tz).date())
        account_trades = await self._get_account_trades(dates)
        account_trades.sort(key=lambda trade: self._as_datetime(trade.timestamp))

        candidates: dict[tuple[str, LimeOrderSide], list[Order]] = defaultdict(list)
        remaining: dict[str, int] = {}
        for order in open_orders:
            details = details_by_id[order.exchange_order_id]
            key = (details.symbol, details.order_side)
            candidates[key].append(order)
            remaining[order.exchange_order_id] = max(
                0,
                int(abs(details.executed_quantity))
                - self._reported_filled_quantity[order.exchange_order_id],
            )
        for candidate_orders in candidates.values():
            candidate_orders.sort(key=lambda item: item.dt)

        transactions: list[Transaction] = []
        for trade in account_trades:
            trade_id = str(trade.trade_id)
            if trade_id in self._processed_trade_ids:
                continue
            side = (
                LimeOrderSide.BUY if trade.side == TradeSide.BUY else LimeOrderSide.SELL
            )
            trade_dt = self._as_datetime(trade.timestamp)
            quantity_left = abs(int(trade.quantity))
            allocated = False

            for order in candidates.get((trade.symbol, side), []):
                external_id = order.exchange_order_id
                if remaining[external_id] <= 0:
                    continue
                order_dt = order.dt
                if order_dt.tzinfo is None:
                    order_dt = order_dt.replace(tzinfo=self.trading_calendar.tz)
                if trade_dt < order_dt.astimezone(datetime.timezone.utc):
                    continue

                fill_quantity = min(quantity_left, remaining[external_id])
                if fill_quantity <= 0:
                    continue
                signed_quantity = (
                    fill_quantity if side == LimeOrderSide.BUY else -fill_quantity
                )
                tx_id = (
                    trade_id
                    if fill_quantity == abs(int(trade.quantity))
                    else f"{trade_id}:{external_id}"
                )
                transactions.append(
                    Transaction(
                        id=tx_id,
                        asset=order.asset,
                        amount=signed_quantity,
                        dt=trade_dt.astimezone(self.trading_calendar.tz),
                        price=float(trade.price),
                        order_id=order.id,
                        exchange_name=self.name,
                        commission=None,
                        realized_pnl=0.0,
                        trading_account_id=self.account_id,
                    )
                )
                remaining[external_id] -= fill_quantity
                self._reported_filled_quantity[external_id] += fill_quantity
                quantity_left -= fill_quantity
                allocated = True
                if quantity_left == 0:
                    break

            if allocated:
                self._processed_trade_ids.add(trade_id)

        closed: list[Order] = []
        for order in refreshed_orders:
            details = details_by_id[order.exchange_order_id]
            fully_reported = self._reported_filled_quantity[
                order.exchange_order_id
            ] >= int(abs(details.executed_quantity))
            # Keep a terminal partially-filled order in the blotter until all
            # of its trades have appeared in the account-trades endpoint.  The
            # order-status and trades endpoints are not guaranteed to become
            # consistent in the same request round.
            if details.order_status in _TERMINAL_STATUSES and fully_reported:
                closed.append(order)

        return transactions, [], closed

    async def get_transactions_by_order_ids(
        self, order_ids: list[str]
    ) -> list[Transaction]:
        wanted = set(order_ids)
        known = [
            order
            for external_id, order in self._orders_by_external_id.items()
            if external_id in wanted or order.id in wanted
        ]
        if not known:
            return []
        nested: dict[ExchangeAsset, dict[str, Order]] = defaultdict(dict)
        for order in known:
            nested[order.asset][order.id] = order
        transactions, _, _ = await self.get_transactions(
            orders=nested,
            current_dt=datetime.datetime.now(tz=self.trading_calendar.tz),
            same_bar_execution=True,
        )
        return transactions

    async def cancel_order(self, order_id: str) -> None:
        external_id = order_id
        if external_id not in self._orders_by_external_id:
            by_client_id = next(
                (
                    broker_id
                    for broker_id, order in self._orders_by_external_id.items()
                    if order.id == order_id
                ),
                None,
            )
            if by_client_id is not None:
                external_id = by_client_id

        if self._order_risk_limits is not None:
            if external_id not in self._submitted_order_ids:
                raise ValueError(
                    "risk-controlled Lime cancellation is allowed only for an order "
                    "submitted by this invocation"
                )
            tracked_order = self._orders_by_external_id[external_id]
            symbol = str(tracked_order.asset.symbol).strip().upper()
            allowed = self._order_risk_limits["allowed_symbols"]
            if allowed is not None and symbol not in allowed:
                raise ValueError(
                    f"Lime cancellation symbol {symbol!r} is not allowlisted"
                )

        self._assert_order_deadline()
        response = await self._client.trading.cancel_order(
            order_id=external_id,
            message="Cancelled by ziplime",
        )
        if not response.success:
            raise RuntimeError(
                f"Lime did not accept cancellation for order {external_id!r}."
            )
        if external_id in self._orders_by_external_id:
            self._orders_by_external_id[external_id].cancel()

    # ------------------------------------------------------------------
    # Live and historical market data
    # ------------------------------------------------------------------
    @staticmethod
    def _float(value: Any) -> float | None:
        return None if value is None else float(value)

    @staticmethod
    def _tz_name(value: datetime.datetime) -> str:
        return str(value.tzinfo or datetime.timezone.utc)

    @staticmethod
    def _columns(fields: frozenset[str] | None) -> list[str]:
        requested = fields or _DEFAULT_FIELDS
        preferred = [
            "date",
            "sid",
            "symbol",
            "mic",
            "open",
            "high",
            "low",
            "close",
            "price",
            "volume",
            "ask",
            "ask_size",
            "bid",
            "bid_size",
            "last_size",
            "last_traded",
        ]
        required = set(requested).union({"date", "sid", "symbol"})
        return [column for column in preferred if column in required]

    @classmethod
    def _empty_frame(cls, fields: frozenset[str] | None, tz_name: str) -> pl.DataFrame:
        types: dict[str, pl.DataType] = {
            "date": pl.Datetime(time_zone=tz_name),
            "last_traded": pl.Datetime(time_zone=tz_name),
            "sid": pl.Int64,
            "symbol": pl.String,
            "mic": pl.String,
            "volume": pl.Float64,
        }
        schema = [
            (column, types.get(column, pl.Float64)) for column in cls._columns(fields)
        ]
        return pl.DataFrame(schema=schema)

    async def get_spot_value(
        self,
        assets: frozenset[ExchangeAsset],
        fields: frozenset[str],
        dt: datetime.datetime,
        data_frequency: datetime.timedelta | str | None = None,
    ) -> pl.DataFrame:
        del data_frequency
        if not assets:
            return self._empty_frame(fields, self._tz_name(dt))

        assets_by_symbol = {asset.symbol: asset for asset in assets}
        self._assets_by_symbol.update(assets_by_symbol)
        quotes = await self._client.market.get_current_quotes(
            symbols=list(assets_by_symbol)
        )
        quotes_by_symbol = {quote.symbol: quote for quote in quotes}
        missing = sorted(set(assets_by_symbol) - set(quotes_by_symbol))
        if missing:
            raise RuntimeError(
                f"Lime returned no current quote for: {', '.join(missing)}"
            )

        rows = []
        for symbol, asset in assets_by_symbol.items():
            quote = quotes_by_symbol[symbol]
            quote_dt = self._as_datetime(quote.date).astimezone(
                dt.tzinfo or self.trading_calendar.tz
            )
            self._last_traded_by_symbol[symbol] = quote_dt
            last = quote.last if quote.last is not None else quote.close
            rows.append(
                {
                    "date": quote_dt,
                    "last_traded": quote_dt,
                    "sid": asset.sid,
                    "symbol": symbol,
                    "mic": asset.mic,
                    "open": self._float(quote.open),
                    "high": self._float(quote.high),
                    "low": self._float(quote.low),
                    # For a live bar, close/price are the most recent trade.  The
                    # SDK's quote.close is yesterday's close.
                    "close": self._float(last),
                    "price": self._float(last),
                    "volume": self._float(quote.volume),
                    "ask": self._float(quote.ask),
                    "ask_size": self._float(quote.ask_size),
                    "bid": self._float(quote.bid),
                    "bid_size": self._float(quote.bid_size),
                    "last_size": self._float(quote.last_size),
                }
            )
        return pl.DataFrame(rows).select(self._columns(fields))

    @staticmethod
    def _frequency_to_timedelta(
        frequency: datetime.timedelta | str,
    ) -> datetime.timedelta:
        if isinstance(frequency, datetime.timedelta):
            return frequency
        match = re.fullmatch(
            r"([1-9][0-9]*)(us|ms|s|m|h|d|w|mo|q|y)", str(frequency).lower()
        )
        if not match:
            raise ValueError(f"Unsupported Lime market-data frequency: {frequency!r}")
        count = int(match.group(1))
        unit = match.group(2)
        factors = {
            "us": datetime.timedelta(microseconds=1),
            "ms": datetime.timedelta(milliseconds=1),
            "s": datetime.timedelta(seconds=1),
            "m": datetime.timedelta(minutes=1),
            "h": datetime.timedelta(hours=1),
            "d": datetime.timedelta(days=1),
            "w": datetime.timedelta(weeks=1),
            "mo": datetime.timedelta(days=30),
            "q": datetime.timedelta(days=90),
            "y": datetime.timedelta(days=365),
        }
        return factors[unit] * count

    @classmethod
    def _frequency_to_period(cls, frequency: datetime.timedelta | str) -> LimePeriod:
        delta = cls._frequency_to_timedelta(frequency)
        periods = {
            datetime.timedelta(minutes=1): LimePeriod.MINUTE,
            datetime.timedelta(minutes=5): LimePeriod.MINUTE_5,
            datetime.timedelta(minutes=15): LimePeriod.MINUTE_15,
            datetime.timedelta(minutes=30): LimePeriod.MINUTE_30,
            datetime.timedelta(hours=1): LimePeriod.HOUR,
            datetime.timedelta(days=1): LimePeriod.DAY,
            datetime.timedelta(weeks=1): LimePeriod.WEEK,
            datetime.timedelta(days=30): LimePeriod.MONTH,
            datetime.timedelta(days=90): LimePeriod.QUARTER,
            datetime.timedelta(days=365): LimePeriod.YEAR,
        }
        try:
            return periods[delta]
        except KeyError as exc:
            supported = "1m, 5m, 15m, 30m, 1h, 1d, 1w, 1mo, 1q, 1y"
            raise ValueError(
                f"Frequency {frequency!r} is not supported by Lime Trader SDK; use {supported}."
            ) from exc

    def _history_start(
        self,
        *,
        end_date: datetime.datetime,
        frequency: datetime.timedelta,
        limit: int,
    ) -> datetime.datetime:
        end_ts = pd.Timestamp(end_date)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize(self.trading_calendar.tz)

        try:
            if frequency == datetime.timedelta(days=1):
                session = self.trading_calendar.date_to_session(
                    end_ts.date(), direction="previous"
                )
                sessions = self.trading_calendar.sessions_window(
                    session, -max(limit, 1)
                )
                first_session = sessions[0]
                return datetime.datetime.combine(
                    first_session.date(), datetime.time.min, tzinfo=end_ts.tzinfo
                )
            if frequency < datetime.timedelta(days=1):
                if self.trading_calendar.is_open_on_minute(end_ts):
                    end_minute = end_ts
                else:
                    end_minute = self.trading_calendar.previous_minute(end_ts)
                multiplier = max(1, int(frequency / datetime.timedelta(minutes=1)))
                count = limit * multiplier
                minutes = self.trading_calendar.minutes_window(
                    end_minute, -max(count, 1)
                )
                first_minute = minutes[0]
                return first_minute.to_pydatetime().astimezone(end_ts.tzinfo)
        except (ValueError, IndexError, KeyError):
            # Calendar boundaries can be exceeded for very long requests.  The
            # conservative fallback merely asks Lime for a larger date range.
            pass

        return end_date - frequency * max(limit * 2, 2)

    @staticmethod
    def _history_max_span(period: LimePeriod) -> datetime.timedelta:
        """Conservative per-request ranges documented by Lime.

        The API rejects wider windows, even when the requested number of bars
        is small (for example seven NYSE sessions span more than one calendar
        week).  Slightly sub-limit spans avoid boundary/rounding surprises.
        """
        if period == LimePeriod.MINUTE:
            return datetime.timedelta(days=6, hours=23)
        if period in {
            LimePeriod.MINUTE_5,
            LimePeriod.MINUTE_15,
            LimePeriod.MINUTE_30,
            LimePeriod.HOUR,
        }:
            return datetime.timedelta(days=29)
        if period == LimePeriod.DAY:
            return datetime.timedelta(days=364)
        return datetime.timedelta(days=5 * 365 - 1)

    @classmethod
    def _history_windows(
        cls,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        period: LimePeriod,
    ) -> list[tuple[datetime.datetime, datetime.datetime]]:
        if date_from >= date_to:
            return []
        max_span = cls._history_max_span(period)
        windows: list[tuple[datetime.datetime, datetime.datetime]] = []
        cursor = date_from
        while cursor < date_to:
            window_end = min(cursor + max_span, date_to)
            windows.append((cursor, window_end))
            cursor = window_end
        return windows

    def _cap_history_end(self, date_to: datetime.datetime) -> datetime.datetime:
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=self.trading_calendar.tz)
        now = datetime.datetime.now(tz=date_to.tzinfo)
        return min(date_to, now)

    async def get_data(
        self,
        assets: list[ExchangeAsset] | frozenset[ExchangeAsset],
        frequency: datetime.timedelta | str,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        **kwargs,
    ) -> pl.DataFrame:
        del kwargs
        assets = list(assets)
        fields = _DEFAULT_FIELDS
        tz_name = self._tz_name(date_from)
        if not assets:
            return self._empty_frame(fields, tz_name)

        period = self._frequency_to_period(frequency)
        date_to = self._cap_history_end(date_to)
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=date_to.tzinfo)
        windows = self._history_windows(date_from, date_to, period)
        if not windows:
            return self._empty_frame(fields, tz_name)

        self._assets_by_symbol.update({asset.symbol: asset for asset in assets})

        async def fetch_asset_history(asset: ExchangeAsset):
            parts = await asyncio.gather(
                *(
                    self._client.market.get_quotes_history(
                        symbol=asset.symbol,
                        period=period,
                        from_date=window_start,
                        to_date=window_end,
                    )
                    for window_start, window_end in windows
                )
            )
            return [bar for part in parts for bar in part]

        results = await asyncio.gather(
            *(fetch_asset_history(asset) for asset in assets)
        )

        rows = []
        target_tz = date_from.tzinfo or self.trading_calendar.tz
        for asset, history in zip(assets, results):
            for bar in history:
                bar_dt = self._as_datetime(bar.timestamp).astimezone(target_tz)
                rows.append(
                    {
                        "date": bar_dt,
                        "sid": asset.sid,
                        "symbol": asset.symbol,
                        "mic": asset.mic,
                        "open": float(bar.open),
                        "high": float(bar.high),
                        "low": float(bar.low),
                        "close": float(bar.close),
                        "price": float(bar.close),
                        "volume": float(bar.volume),
                    }
                )
        if not rows:
            return self._empty_frame(fields, tz_name)
        return (
            pl.DataFrame(rows)
            .unique(subset=["sid", "date"], keep="last")
            .sort(["date", "sid"])
        )

    async def get_data_by_limit(
        self,
        fields: frozenset[str] | None,
        limit: int,
        end_date: datetime.datetime,
        frequency: datetime.timedelta | str,
        assets: frozenset[ExchangeAsset],
        include_end_date: bool,
    ) -> pl.DataFrame:
        if limit <= 0:
            return self._empty_frame(fields, self._tz_name(end_date))
        delta = self._frequency_to_timedelta(frequency)
        effective_end = self._cap_history_end(end_date)
        date_from = self._history_start(
            end_date=effective_end, frequency=delta, limit=limit
        )
        frame = await self.get_data(
            assets=assets,
            frequency=frequency,
            date_from=date_from,
            date_to=effective_end,
        )
        if frame.is_empty():
            return self._empty_frame(fields, self._tz_name(end_date))

        comparator = (
            pl.col("date") <= end_date
            if include_end_date
            else pl.col("date") < end_date
        )
        frame = frame.filter(comparator)
        if frame.is_empty():
            return self._empty_frame(fields, self._tz_name(end_date))
        frame = (
            frame.sort(["sid", "date"]).group_by("sid", maintain_order=True).tail(limit)
        )
        return frame.sort(["date", "sid"]).select(self._columns(fields))

    def get_last_traded_dt(self, asset: ExchangeAsset):
        return self._last_traded_by_symbol.get(asset.symbol)

    def get_slippage_model(self, asset: ExchangeAsset):
        del asset
        return NoSlippage()

    def get_commission_model(self, asset: ExchangeAsset) -> CommissionModel:
        return self.commission_models.get(type(asset.asset), NoCommission())

    async def close(self) -> None:
        """Close SDK HTTP clients (useful for short-lived smoke checks/tests)."""
        seen: set[int] = set()
        for api_name in ("_api_client", "_auth_api_client"):
            api = getattr(self._client, api_name, None)
            http_client = getattr(api, "_http_client", None)
            close = getattr(http_client, "aclose", None)
            if close is not None and id(http_client) not in seen:
                seen.add(id(http_client))
                await close()
