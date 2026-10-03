"""The helpers a strategy is written with, each covering a paragraph it used to need.

`data.returns`, `context.rebalance`, `context.front_contract` and
`context.is_month_start` exist to keep the arithmetic of a cross-sectional or a
futures strategy out of user code -- the twelve lines that turn a long price
frame into a ranking, the loop that forgets to close what left the universe, the
chain that is not filtered by the simulation date, the remembered month. Each one
is exercised here directly, without an engine: they are small, and what matters
is the edge they were written for.
"""
import datetime
import types
import unittest

import polars as pl

from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm


class Listing:
    """Enough of an ``ExchangeAsset`` for these: a sid, a name, and hashable by sid."""

    def __init__(self, sid: int, symbol: str = "X", mic: str = "MISX", asset=None):
        self.sid, self.symbol, self.mic = sid, symbol, mic
        # Every real listing carries the instrument it lists; an equity by default here.
        self.asset = asset if asset is not None else types.SimpleNamespace(multiplier=1.0)

    def __hash__(self):
        return hash(self.sid)

    def __eq__(self, other):
        return getattr(other, "sid", None) == self.sid


def listing(sid: int, symbol: str = "X"):
    return Listing(sid, symbol)


class ReturnsTests(unittest.IsolatedAsyncioTestCase):
    """`data.returns` answers per listing, and stays silent about what it cannot price."""

    @staticmethod
    async def _returns(rows, assets, bar_count=5):
        frame = pl.DataFrame(rows) if rows else pl.DataFrame(
            {"sid": [], "date": [], "close": []})

        async def history(**kwargs):
            return frame

        stub = types.SimpleNamespace(history=history)
        return await BarData.returns(stub, assets=assets, bar_count=bar_count)

    async def test_the_window_is_read_end_to_end(self):
        rows = [{"sid": 1, "date": d, "close": price}
                for d, price in enumerate([100.0, 110.0, 120.0])]
        result = await self._returns(rows, [listing(1)])

        self.assertAlmostEqual(list(result.values())[0], 0.2)

    async def test_a_listing_with_no_bars_is_absent_rather_than_zero(self):
        rows = [{"sid": 1, "date": 0, "close": 100.0}, {"sid": 1, "date": 1, "close": 90.0}]
        one, two = listing(1), listing(2)

        result = await self._returns(rows, [one, two])

        self.assertEqual(list(result), [one], "an asset the vendor never filled is not a -100%")

    async def test_zeros_are_not_prices(self):
        """A contract that had not listed yet is forward-filled with zeros."""
        rows = [{"sid": 1, "date": 0, "close": 0.0}, {"sid": 1, "date": 1, "close": 0.0},
                {"sid": 1, "date": 2, "close": 50.0}, {"sid": 1, "date": 3, "close": 60.0}]

        result = await self._returns(rows, [listing(1)])

        self.assertAlmostEqual(list(result.values())[0], 0.2, msg="measured from the first real bar")

    async def test_nothing_to_read_is_not_an_error(self):
        self.assertEqual(await self._returns([], [listing(1)]), {})
        self.assertEqual(await self._returns([], []), {})


class RebalanceTests(unittest.IsolatedAsyncioTestCase):
    """`context.rebalance` holds what it was given and closes everything else."""

    @staticmethod
    def _algorithm(held, futures=False, price=100.0, portfolio_value=1_000_000.0):
        ordered = {}
        amounts = {}

        async def order_target_percent(asset, target, style=None, exchange_name=None):
            ordered[asset] = target

        async def order(asset, amount, style=None, exchange_name=None):
            amounts[asset] = amount

        async def get_asset_positions_amount(asset, exchange_name=None, trading_account_id=None):
            return held.get(asset, 0)

        async def current(assets, fields):
            return pl.DataFrame({"sid": [a.sid for a in assets], "price": [price] * len(assets)})

        positions = {(None, None, asset): types.SimpleNamespace(asset=asset, amount=amount)
                     for asset, amount in held.items()}
        stub = types.SimpleNamespace(
            portfolio=types.SimpleNamespace(
                positions=positions, portfolio_value=portfolio_value,
                get_asset_positions_amount=get_asset_positions_amount),
            current_data=types.SimpleNamespace(current=current),
            contracts_for_notional=lambda asset, notional, price: int(notional / price),
            order=order,
            order_target_percent=order_target_percent)
        return stub, ordered, amounts

    async def test_the_named_listings_get_their_weight(self):
        one, two = listing(1), listing(2)
        stub, ordered, _amounts = self._algorithm({})

        await TradingAlgorithm.rebalance(stub, {one: 0.6, two: 0.4})

        self.assertEqual(ordered, {one: 0.6, two: 0.4})

    async def test_what_is_held_and_not_named_is_closed(self):
        """The half a hand-written loop forgets, which leaves a dropped name in the book."""
        one, two = listing(1), listing(2)
        stub, ordered, amounts = self._algorithm({one: 10, two: 5})

        await TradingAlgorithm.rebalance(stub, {one: 1.0})

        self.assertEqual(ordered, {one: 1.0})
        self.assertEqual(amounts, {two: -5}, "closing is subtraction, not a target")

    async def test_an_empty_book_is_asked_for_nothing(self):
        stub, ordered, amounts = self._algorithm({listing(1): 0})

        await TradingAlgorithm.rebalance(stub, {})

        self.assertEqual((ordered, amounts), ({}, {}), "a position of zero is not a position")

    async def test_a_contract_is_sized_by_notional_rather_than_by_percent(self):
        """`order_target_percent` asks the futures slippage model a question it cannot answer."""
        contract = Listing(7, "SiZ6")
        contract.asset = FuturesContract.__new__(FuturesContract)
        stub, ordered, amounts = self._algorithm({}, price=100.0, portfolio_value=1_000.0)

        await TradingAlgorithm.rebalance(stub, {contract: 0.5})

        self.assertEqual(ordered, {}, "no percentage target is sent for a contract")
        self.assertEqual(amounts, {contract: 5}, "500 of notional at 100 a contract")

    async def test_a_contract_already_at_its_size_is_left_alone(self):
        contract = Listing(7, "SiZ6")
        contract.asset = FuturesContract.__new__(FuturesContract)
        stub, ordered, amounts = self._algorithm({contract: 5}, price=100.0,
                                                 portfolio_value=1_000.0)

        await TradingAlgorithm.rebalance(stub, {contract: 0.5})

        self.assertEqual((ordered, amounts), ({}, {}))


class PeriodStartTests(unittest.TestCase):
    """`is_month_start` reads the calendar rather than arithmetic on dates."""

    @staticmethod
    def _algorithm(today, previous):
        calendar = types.SimpleNamespace(previous_session=lambda session: previous)
        return types.SimpleNamespace(
            clock=types.SimpleNamespace(trading_calendar=calendar),
            _simulation_date=lambda: today,
            _is_period_start=TradingAlgorithm._is_period_start.__get__(
                types.SimpleNamespace()))

    def test_the_first_session_of_a_month_opens_the_gate(self):
        stub = types.SimpleNamespace(
            clock=types.SimpleNamespace(trading_calendar=types.SimpleNamespace(
                previous_session=lambda session: datetime.date(2026, 8, 31))),
            _simulation_date=lambda: datetime.date(2026, 9, 1))

        self.assertTrue(TradingAlgorithm._is_period_start(stub, unit="month"))

    def test_an_ordinary_session_does_not(self):
        stub = types.SimpleNamespace(
            clock=types.SimpleNamespace(trading_calendar=types.SimpleNamespace(
                previous_session=lambda session: datetime.date(2026, 9, 1))),
            _simulation_date=lambda: datetime.date(2026, 9, 2))

        self.assertFalse(TradingAlgorithm._is_period_start(stub, unit="month"))

    def test_a_holiday_does_not_move_the_gate(self):
        """The first of September falls on a Sunday in 2030; the gate opens on the 2nd."""
        stub = types.SimpleNamespace(
            clock=types.SimpleNamespace(trading_calendar=types.SimpleNamespace(
                previous_session=lambda session: datetime.date(2030, 8, 30))),
            _simulation_date=lambda: datetime.date(2030, 9, 2))

        self.assertTrue(TradingAlgorithm._is_period_start(stub, unit="month"))

    def test_the_week_gate_follows_iso_weeks(self):
        stub = types.SimpleNamespace(
            clock=types.SimpleNamespace(trading_calendar=types.SimpleNamespace(
                previous_session=lambda session: datetime.date(2026, 9, 18))),
            _simulation_date=lambda: datetime.date(2026, 9, 21))

        self.assertTrue(TradingAlgorithm._is_period_start(stub, unit="week"))


class FrontContractTests(unittest.IsolatedAsyncioTestCase):
    """`front_contract` picks the delivery to hold, and rolling is asking again."""

    @staticmethod
    def _algorithm(today, expirations):
        chain = [types.SimpleNamespace(
            sid=index, symbol=f"Si{index}",
            asset=types.SimpleNamespace(expiration_date=expiry))
            for index, expiry in enumerate(expirations)]

        async def futures_chain(root_symbol, mic=None):
            return chain

        return types.SimpleNamespace(
            futures_chain=futures_chain, _simulation_date=lambda: today), chain

    async def test_the_nearest_live_delivery_is_the_front(self):
        stub, chain = self._algorithm(
            datetime.date(2026, 9, 21),
            [datetime.date(2026, 3, 19), datetime.date(2026, 12, 17), datetime.date(2027, 3, 18)])

        front = await TradingAlgorithm.front_contract(stub, "Si")

        self.assertIs(front, chain[1], "the settled delivery is skipped")

    async def test_the_roll_offset_moves_the_answer_on(self):
        """Within the offset of expiry the answer is the next delivery: that is the roll."""
        stub, chain = self._algorithm(
            datetime.date(2026, 12, 15),
            [datetime.date(2026, 12, 17), datetime.date(2027, 3, 18)])

        self.assertIs(await TradingAlgorithm.front_contract(stub, "Si", roll_before_days=0),
                      chain[0])
        self.assertIs(await TradingAlgorithm.front_contract(stub, "Si", roll_before_days=7),
                      chain[1])

    async def test_a_chain_with_nothing_left_answers_none(self):
        stub, _chain = self._algorithm(datetime.date(2026, 9, 21),
                                       [datetime.date(2026, 3, 19)])

        self.assertIsNone(await TradingAlgorithm.front_contract(stub, "Si"))


if __name__ == "__main__":
    unittest.main()
