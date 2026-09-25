import datetime
import unittest
from unittest.mock import Mock

from exchange_calendars import get_calendar

from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.execution import MarketOrder
from ziplime.gens.domain.trading_clock import TradingClock


class FixtureClock(TradingClock):
    def __iter__(self):
        return iter(())


def make_exchange(cash=10_000.0):
    calendar = get_calendar("XNYS")
    return SimulationExchange(
        name="SIM",
        country_code="US",
        trading_calendar=calendar,
        clock=FixtureClock(calendar, datetime.timedelta(days=1)),
        cash_balance=cash,
        equity_slippage=Mock(),
        future_slippage=Mock(),
        equity_commission=Mock(),
        future_commission=Mock(),
        account_id="account",
        is_default=True,
    )


class SimulationExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_starts_with_cash_only(self):
        exchange = make_exchange()

        self.assertEqual(exchange.get_start_cash_balance(), 10_000.0)
        self.assertEqual(exchange.get_current_cash_balance(), 10_000.0)
        account = await exchange.get_account()
        self.assertEqual(account.settled_cash, 10_000.0)
        self.assertEqual(account.net_liquidation, 10_000.0)
        self.assertEqual(account.total_positions_value, 0.0)
    async def test_order_is_returned_without_exchange_tracking(self):
        exchange = make_exchange()
        asset = Mock()
        asset.symbol = "TEST"
        asset.mic = "SIM"

        order = await exchange.order(asset, 3, MarketOrder())

        self.assertTrue(order.open)
        await exchange.cancel_order(order.id)
        self.assertTrue(order.open)
