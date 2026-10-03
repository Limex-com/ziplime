import datetime
import unittest
from unittest.mock import Mock

import polars as pl
from exchange_calendars import get_calendar

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
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

    async def test_get_data_by_period_forwards_date_bounds_to_sync_source(self):
        exchange = make_exchange()
        start = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
        end = datetime.datetime(2024, 1, 4, tzinfo=datetime.UTC)
        fields = frozenset({"close"})
        assets = frozenset({Mock()})
        frequency = datetime.timedelta(days=1)
        expected = pl.DataFrame({"close": [12.0]})
        exchange.data_source = Mock()
        exchange.data_source.get_data_by_date.return_value = expected
        await type(exchange).get_data_by_period.cache.clear()
        self.addAsyncCleanup(type(exchange).get_data_by_period.cache.clear)

        result = await exchange.get_data_by_period(
            fields=fields, start_date=start, end_date=end, frequency=frequency,
            assets=assets, include_end_date=False, source="prices")

        exchange.data_source.get_data_by_date.assert_called_once_with(
            fields=fields, from_date=start, to_date=end, frequency=frequency,
            assets=assets, include_bounds=False)
        self.assertIs(result, expected)

    async def test_get_data_by_period_reads_bundle_with_inclusive_and_exclusive_bounds(self):
        exchange = make_exchange()
        start = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
        middle = start + datetime.timedelta(days=1)
        end = start + datetime.timedelta(days=2)
        frequency = datetime.timedelta(days=1)
        exchange.data_source = DataBundle(
            name="prices", version="1", start_date=start, end_date=end, timestamp=start,
            trading_calendar=get_calendar("XNYS"), frequency=frequency, original_frequency=frequency,
            data_type=DataType.MARKET_DATA, sid_indexes={7: (0, 3)},
            data=pl.DataFrame({"sid": [7, 7, 7], "date": [start, middle, end], "close": [10.0, 11.0, 12.0]}))
        assets = frozenset({Mock(spec=ExchangeAsset, sid=7)})
        await type(exchange).get_data_by_period.cache.clear()
        self.addAsyncCleanup(type(exchange).get_data_by_period.cache.clear)

        for include_bounds, expected in ((True, [10.0, 11.0, 12.0]), (False, [11.0])):
            with self.subTest(include_bounds=include_bounds):
                result = await exchange.get_data_by_period(
                    fields=frozenset({"close"}), start_date=start, end_date=end, frequency=frequency,
                    assets=assets, include_end_date=include_bounds, source="prices")

                self.assertEqual(result["close"].to_list(), expected)
                self.assertEqual(result["sid"].to_list(), [7] * len(expected))
