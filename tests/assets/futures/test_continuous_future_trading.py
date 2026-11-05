"""A continuous future traded as one instrument: orders go to the current contract, and the
engine moves the position when the front changes.

Before, a strategy had to remember which contract it held, compare it with ``front_contract``
every bar, and close one and open the other itself. These pin what replaces that: the order
methods accept the specifier, ``_roll_continuous_futures`` (run before every ``handle_data``)
rolls, the portfolio reports the chain's position, and each specifier rolls on its own offset.
"""
import datetime
import types
import unittest

from tests.futures_fixtures import make_bundle, make_future, session

from ziplime.domain.portfolio import Portfolio
from ziplime.trading.trading_algorithm import TradingAlgorithm, _resolves_continuous_future

SESSIONS = [datetime.date(2023, 6, d) for d in (5, 6, 7, 8, 9, 12, 13, 14, 15, 16)]


def chain():
    return [make_future(sid=1, symbol="CLN3", expiration=datetime.date(2023, 6, 20)),
            make_future(sid=2, symbol="CLU3", expiration=datetime.date(2023, 9, 20))]


class Engine:
    """Enough of TradingAlgorithm for the roll: a clock, the bundle, a position book, orders."""

    _contract_for = TradingAlgorithm._contract_for
    _roll_continuous_futures = TradingAlgorithm._roll_continuous_futures

    def __init__(self, bundle):
        self.bundle, self.day, self.positions, self.orders = bundle, SESSIONS[0], {}, []
        self._continuous_holdings = {}
        self._logger = types.SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None)
        engine = self

        class CurrentData:
            async def current_contract(self, continuous_future):
                return await engine.bundle.current_contract(continuous_future,
                                                            session(engine.bundle, engine.day))

        class Book:
            async def get_asset_positions_amount(self, asset):
                return engine.positions.get(asset.sid, 0)

        self.current_data, self.portfolio = CurrentData(), Book()

    @property
    def simulation_dt(self):
        return session(self.bundle, self.day)

    @_resolves_continuous_future
    async def order(self, asset, amount, style=None):
        self.orders.append((self.day, asset.symbol, amount))
        self.positions[asset.sid] = self.positions.get(asset.sid, 0) + amount
        return asset.symbol


class ContinuousFutureTradingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        contracts = chain()
        self.bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in contracts})
        self.engine = Engine(self.bundle)

    async def cf(self, days):
        return await self.bundle.asset_service.create_continuous_future(
            "CL", 0, "calendar", "mul", roll_offset_days=days)

    async def test_an_order_on_the_specifier_goes_to_the_current_contract(self):
        cf = await self.cf(7)
        self.assertEqual(await self.engine.order(cf, 2), "CLN3")
        self.assertEqual(self.engine._continuous_holdings[cf.sid][1].symbol, "CLN3")

    async def test_the_position_moves_when_the_front_changes(self):
        # CLN3 expires 06-20 and is closed out on 06-21; seven days before that is 06-14.
        cf = await self.cf(7)
        await self.engine.order(cf, 2)
        for day in SESSIONS[1:]:
            self.engine.day = day
            await self.engine._roll_continuous_futures()
        self.assertEqual(self.engine.orders, [
            (SESSIONS[0], "CLN3", 2),
            (datetime.date(2023, 6, 14), "CLN3", -2),
            (datetime.date(2023, 6, 14), "CLU3", 2),
        ])
        self.assertEqual(self.engine.positions, {1: 0, 2: 2})

    async def test_each_specifier_rolls_on_its_own_offset(self):
        near, far = await self.cf(1), await self.cf(10)
        self.assertNotEqual(near.sid, far.sid)
        day = session(self.bundle, datetime.date(2023, 6, 12))
        self.assertEqual((await self.bundle.current_contract(near, day)).symbol, "CLN3")
        self.assertEqual((await self.bundle.current_contract(far, day)).symbol, "CLU3")

    async def test_a_flat_specifier_is_followed_but_not_traded(self):
        cf = await self.cf(7)
        await self.engine.order(cf, 2)
        await self.engine.order(self.engine._continuous_holdings[cf.sid][1], -2)
        self.engine.day = datetime.date(2023, 6, 14)
        await self.engine._roll_continuous_futures()
        self.assertEqual(len(self.engine.orders), 2, "nothing to roll, so no orders")
        self.assertEqual(self.engine._continuous_holdings[cf.sid][1].symbol, "CLU3")

    async def test_a_chain_of_one_never_rolls(self):
        perpetual = make_future(sid=5, symbol="IMOEXF", root_symbol="IMOEXF",
                                expiration=datetime.date(2099, 1, 1))
        bundle = make_bundle({perpetual: [(d, 2500.0, 10.0) for d in SESSIONS]})
        engine = Engine(bundle)
        cf = await bundle.asset_service.create_continuous_future("IMOEXF", 0, "calendar", "mul",
                                                                 roll_offset_days=7)
        await engine.order(cf, 1)
        for day in SESSIONS[1:]:
            engine.day = day
            await engine._roll_continuous_futures()
        self.assertEqual(engine.orders, [(SESSIONS[0], "IMOEXF", 1)])


class SidTests(unittest.TestCase):
    def test_a_long_root_still_fits_int64(self):
        from ziplime.assets.utils import _encode_continuous_future_sid
        sid = _encode_continuous_future_sid("IMOEXF", 0, "calendar", "div", mic="RTSX",
                                            roll_offset_days=55)
        self.assertLess(sid, 2 ** 63)
        self.assertGreaterEqual(sid, 2 ** 62, "kept clear of every listing's sid")

    def test_the_sid_is_deterministic_and_tells_specifiers_apart(self):
        from ziplime.assets.utils import _encode_continuous_future_sid as encode
        self.assertEqual(encode("Si", 0, "calendar", "div", "RTSX", 7),
                         encode("Si", 0, "calendar", "div", "RTSX", 7))
        others = {encode("Si", 0, "calendar", "div", "RTSX", 1), encode("Si", 1, "calendar", "div", "RTSX", 7),
                  encode("Si", 0, "volume", "div", "RTSX", 7), encode("SI", 0, "calendar", "div", "XCEC", 7),
                  encode("Si", 0, "calendar", "add", "RTSX", 7)}
        self.assertNotIn(encode("Si", 0, "calendar", "div", "RTSX", 7), others)
        self.assertEqual(len(others), 5)

    async def _splice(self):
        perpetual = make_future(sid=5, symbol="IMOEXF", root_symbol="IMOEXF",
                                expiration=datetime.date(2099, 1, 1))
        bundle = make_bundle({perpetual: [(d, 2500.0 + i, 10.0) for i, d in enumerate(SESSIONS)]})
        cf = await bundle.asset_service.create_continuous_future("IMOEXF", 0, "calendar", "mul",
                                                                 roll_offset_days=7)
        return await bundle.get_data_by_limit(
            fields=frozenset({"close"}), limit=5, end_date=session(bundle, SESSIONS[-1]),
            frequency=datetime.timedelta(days=1), assets=frozenset({cf}), include_end_date=True)

    def test_the_history_of_a_long_root_splices(self):
        import asyncio
        frame = asyncio.run(self._splice())
        self.assertEqual(frame.height, 5)


class PortfolioChainTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_chain_position_is_whatever_contract_holds_it(self):
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain()})
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "calendar", "mul")
        contracts = chain()
        portfolio = Portfolio.__new__(Portfolio)
        positions = [types.SimpleNamespace(asset=contracts[1], amount=3, last_sale_price=71.0,
                                           exchange_name="X", trading_account_id="A"),
                     types.SimpleNamespace(asset=make_future(sid=9, symbol="NGQ3", root_symbol="NG"),
                                           amount=7, last_sale_price=2.0,
                                           exchange_name="X", trading_account_id="A")]
        portfolio._all_positions = lambda: positions
        self.assertEqual(await portfolio.get_asset_positions_amount(cf), 3)
        self.assertEqual(await portfolio.get_asset_positions_value(cf), 3 * 71.0)


if __name__ == "__main__":
    unittest.main()
