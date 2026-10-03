"""One live tick on Lime through the whole engine, with a position already on the account.

The unit tests of the connector pin each broker call. This pins how the engine uses them
together, which is where a live run breaks: the ledger reconciling the broker portfolio before
the first bar, ``compute_signals`` reading its warm-up from the venue, the bar pricing off live
quotes, and the order reaching the broker -- all in one tick driven by ``SingleTickClock``.
"""
import dataclasses
import datetime
from decimal import Decimal

import pytest

pytest.importorskip("lime_trader", reason="lime-trader-sdk must be installed")

from lime_trader.models.market import Period, Quote, QuoteHistory  # noqa: E402
from lime_trader.models.trading import OrderStatus as LimeOrderStatus  # noqa: E402
from lime_trader.models.trading import PlaceOrderResponse  # noqa: E402

from tests.exchanges.test_lime_trader_sdk_exchange import (  # noqa: E402
    ACCOUNT_NUMBER, FakeAsyncLimeClient, FakeTradingApi, _details, _position,
)
from tests.vectorized.test_vectorized_signals import (  # noqa: E402
    CALENDAR, CASH, END, FIXTURES, START, TICKERS, SignalsTestCase,
)
from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import (  # noqa: E402
    LimeTraderSdkExchange,
)
from ziplime.gens.domain.single_tick_clock import SingleTickClock  # noqa: E402


class FillingTradingApi(FakeTradingApi):
    """A broker that fills every order it accepts, and can say so when asked."""

    async def place_order(self, *, order):
        self.placed_orders.append(order)
        order_id = f"broker-order-{len(self.placed_orders)}"
        details = _details(order_id=order_id, client_order_id=order.client_order_id,
                           status=LimeOrderStatus.FILLED, quantity=str(order.quantity),
                           executed_quantity=str(order.quantity))
        self.details_by_id[order_id] = dataclasses.replace(details, symbol=order.symbol,
                                                           order_side=order.side)
        return PlaceOrderResponse(success=True, data=order_id)


class LimeLiveTickTests(SignalsTestCase):
    tick_dt = datetime.datetime(2024, 9, 30, 12, 0)

    def lime_client(self, held: dict[str, int]) -> FakeAsyncLimeClient:
        client = FakeAsyncLimeClient(positions=[_position(symbol, quantity)
                                                for symbol, quantity in held.items()])
        client.trading = FillingTradingApi()
        for ticker in TICKERS:
            prices = self.prices[ticker]
            client.market.history_by_symbol[ticker] = [
                QuoteHistory(timestamp=stamp.to_pydatetime(), period=Period.DAY,
                             open=Decimal(str(p)), high=Decimal(str(p)), low=Decimal(str(p)),
                             close=Decimal(str(p)), volume=1_000_000)
                for stamp, p in prices.items()]
            last = Decimal(str(round(float(prices.iloc[-1]), 2)))
            client.market.quotes.append(Quote(
                symbol=ticker, ask=last, ask_size=Decimal("10"), bid=last, bid_size=Decimal("10"),
                last=last, last_size=Decimal("1"), volume=1_000_000,
                date=self.tick_dt.replace(tzinfo=self.calendar.tz), high=last, low=last,
                open=last, close=last, week52_high=last, week52_low=last, change=Decimal("0"),
                change_pc=Decimal("0"), open_interest=Decimal("0"), implied_volatility=Decimal("0"),
                theoretical_price=Decimal("0"), delta=Decimal("0"), gamma=Decimal("0"),
                theta=Decimal("0"), vega=Decimal("0")))
        return client

    async def run_tick(self, client: FakeAsyncLimeClient):
        from ziplime.core.run_simulation import run_simulation

        tick_dt = self.tick_dt.replace(tzinfo=self.calendar.tz)
        clock = SingleTickClock(trading_calendar=self.calendar,
                                emission_rate=datetime.timedelta(days=1), tick_dt=tick_dt)
        exchange = LimeTraderSdkExchange(
            name="LIME", canonical_name="LIME", country_code="US", clock=clock,
            trading_calendar=self.calendar, start_cash_balance=CASH,
            asset_service=self.asset_service, is_default=True, account_id=ACCOUNT_NUMBER,
            default_mic="XNYS", client=client, capital_limit=CASH)
        result = await run_simulation(
            start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=CASH, market_data_source=self.bundle(), custom_data_sources=[],
            algorithm_file=str(FIXTURES / "signals_vectorised.py"), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
            print_algo=False, clock=clock, exchange=exchange, default_exchange_name="LIME")
        return result, exchange

    async def test_a_tick_holding_a_position_runs_to_completion(self):
        # Keyed by asset alone, the Lime portfolio crashed synchronize_exchange_portfolio
        # before the first bar of any tick that held a position.
        result, exchange = await self.run_tick(self.lime_client(held={"JNJ": 100}))

        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.perf), 1)
        self.assertEqual(exchange.order_failures, [])

    async def test_a_flat_account_trades_on_its_signals(self):
        client = self.lime_client(held={})

        result, exchange = await self.run_tick(client)

        self.assertEqual(result.errors, [])
        self.assertEqual(len(result.perf), 1)
        self.assertEqual(exchange.order_failures, [])
        # The moving-average cross is long on the tick's session: compute_signals read its
        # warm-up from the venue and the decision reached the broker.
        self.assertTrue(client.trading.placed_orders)
        self.assertLessEqual({o.symbol for o in client.trading.placed_orders}, set(TICKERS))
