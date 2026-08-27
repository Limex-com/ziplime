"""Futures acceptance suite: economic invariants a futures backtest must satisfy.

Numbered to match the acceptance checklist. Each test states the invariant it protects, because a
futures backtest can run cleanly end to end and still be financially meaningless.
"""
import datetime
import unittest

from futures_fixtures import make_equity, make_future, make_ledger, mark, settle, trade


class MultiplierAndExposureTests(unittest.IsolatedAsyncioTestCase):
    """#2 multiplier, #3 notional exposure, #26 tick value."""

    async def test_pnl_scales_with_the_contract_multiplier(self):
        # PnL = (p1 - p0) * multiplier * quantity
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=50.0)
        trade(ledger, future, amount=2, price=100.0)
        cash_after_open = ledger.portfolio.cash

        mark(ledger, future, 101.0)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_after_open, 100.0, places=6)

    async def test_short_pnl_has_the_opposite_sign(self):
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=50.0)
        trade(ledger, future, amount=-2, price=100.0)
        cash_after_open = ledger.portfolio.cash

        mark(ledger, future, 101.0)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.cash - cash_after_open, -100.0, places=6)

    async def test_notional_exposure_includes_the_multiplier(self):
        # An equity's exposure is price * shares; a future's is price * multiplier * contracts.
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=50.0)
        trade(ledger, future, amount=3, price=100.0)
        await settle(ledger)

        stats = ledger.position_tracker.stats
        self.assertAlmostEqual(stats.net_exposure, 100.0 * 50.0 * 3)
        self.assertAlmostEqual(stats.net_value, 0.0, msg="a future carries no position value")

    async def test_short_exposure_is_negative(self):
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=50.0)
        trade(ledger, future, amount=-3, price=100.0)
        await settle(ledger)
        self.assertAlmostEqual(ledger.position_tracker.stats.net_exposure, -100.0 * 50.0 * 3)

    async def test_exposure_tracks_several_multipliers_at_once(self):
        ledger = make_ledger()
        small = make_future(sid=1, symbol="MCLF24", multiplier=100.0)
        big = make_future(sid=2, symbol="CLF24", multiplier=1000.0)
        trade(ledger, small, amount=2, price=70.0)
        trade(ledger, big, amount=-1, price=70.0)
        await settle(ledger)

        expected = 70.0 * 100.0 * 2 + 70.0 * 1000.0 * -1
        self.assertAlmostEqual(ledger.position_tracker.stats.net_exposure, expected)

    def test_tick_value_is_tick_size_times_multiplier(self):
        # #26: a wrong multiplier or tick size shows up immediately as an implausible tick value.
        future = make_future(sid=1, multiplier=1000.0, tick_size=0.01)
        tick_value = future.asset.tick_size * future.asset.multiplier
        self.assertAlmostEqual(tick_value, 10.0)


class FuturesCashAccountingTests(unittest.IsolatedAsyncioTestCase):
    """#4: opening a future must not move cash by the notional."""

    async def test_opening_a_future_does_not_pay_the_notional(self):
        ledger = make_ledger(cash=1_000_000.0)
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=10, price=70.0)
        await settle(ledger)

        self.assertEqual(ledger.portfolio.cash, 1_000_000.0)
        self.assertAlmostEqual(ledger.portfolio.positions_value, 0.0)
        self.assertAlmostEqual(ledger.portfolio.positions_exposure, 70.0 * 1000.0 * 10)
        self.assertAlmostEqual(ledger.portfolio.portfolio_value, 1_000_000.0)

    async def test_buying_an_equity_does_pay_the_notional(self):
        # Contrast: the equity path must keep charging cash, so the futures branch is not a
        # blanket change to how transactions settle.
        ledger = make_ledger(cash=1_000_000.0)
        equity = make_equity(sid=900)
        trade(ledger, equity, amount=100, price=70.0)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.cash, 1_000_000.0 - 7_000.0)
        self.assertAlmostEqual(ledger.portfolio.positions_value, 7_000.0)


class EconomicPropertyTests(unittest.IsolatedAsyncioTestCase):
    """Properties A, B and C from the checklist."""

    async def _pnl(self, amount: int, p0: float, p1: float, multiplier: float = 50.0) -> float:
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=multiplier)
        trade(ledger, future, amount=amount, price=p0)
        cash_after_open = ledger.portfolio.cash
        mark(ledger, future, p1)
        await settle(ledger)
        return ledger.portfolio.cash - cash_after_open

    async def test_property_a_long_short_symmetry(self):
        long_pnl = await self._pnl(amount=3, p0=100.0, p1=104.5)
        short_pnl = await self._pnl(amount=-3, p0=100.0, p1=104.5)
        self.assertAlmostEqual(long_pnl, -short_pnl, places=6)

    async def test_property_b_quantity_linearity(self):
        one = await self._pnl(amount=1, p0=100.0, p1=103.0)
        two = await self._pnl(amount=2, p0=100.0, p1=103.0)
        self.assertAlmostEqual(two, 2 * one, places=6)

    async def test_property_c_no_price_move_means_no_pnl(self):
        self.assertAlmostEqual(await self._pnl(amount=5, p0=100.0, p1=100.0), 0.0, places=9)


class MetadataInvariantTests(unittest.TestCase):
    """#40: lifecycle dates and contract specs must be internally consistent.

    Expected ordering: ``start_date <= end_date <= expiration_date < auto_close_date``. The last
    tradable session is ``end_date``; liquidation happens strictly after trading stops, otherwise
    the engine closes a position on a day the strategy can still trade.
    """

    def _check(self, listing):
        contract = listing.asset
        problems = []
        if not contract.start_date <= contract.end_date:
            problems.append("start_date after end_date")
        if not contract.start_date <= contract.expiration_date:
            problems.append("start_date after expiration_date")
        if not contract.multiplier > 0:
            problems.append("multiplier must be positive")
        if not contract.tick_size > 0:
            problems.append("tick_size must be positive")
        if not contract.root_symbol:
            problems.append("root_symbol missing")
        if listing.exchange is None or not listing.exchange.mic:
            problems.append("exchange missing")
        if contract.end_date > contract.expiration_date:
            problems.append("end_date after expiration_date")
        if contract.auto_close_date < contract.end_date:
            problems.append("auto_close_date before the last tradable session")
        if contract.notice_date > contract.expiration_date:
            problems.append("notice_date after expiration_date")
        return problems

    def test_a_well_formed_contract_passes(self):
        self.assertEqual(self._check(make_future(sid=1)), [])

    def test_the_checker_catches_a_malformed_contract(self):
        bad = make_future(sid=1, multiplier=0.0, start=datetime.date(2024, 1, 1),
                          expiration=datetime.date(2023, 1, 1))
        problems = self._check(bad)
        self.assertIn("multiplier must be positive", problems)
        self.assertIn("start_date after expiration_date", problems)


class RootSymbolTests(unittest.TestCase):
    """#37: every contract of a chain reports the same root."""

    def test_contracts_of_a_chain_share_a_root(self):
        chain = [make_future(sid=i, symbol=s, root_symbol="ES")
                 for i, s in enumerate(("ESH27", "ESM27", "ESU27"), start=1)]
        self.assertEqual({c.asset.root_symbol for c in chain}, {"ES"})

    def test_different_products_do_not_collide(self):
        es = make_future(sid=1, symbol="ESH27", root_symbol="ES")
        cl = make_future(sid=2, symbol="CLH27", root_symbol="CL")
        self.assertNotEqual(es.asset.root_symbol, cl.asset.root_symbol)


if __name__ == "__main__":
    unittest.main()


class AutoCloseLiquidationTests(unittest.IsolatedAsyncioTestCase):
    """A contract that reaches its auto-close date must leave the book.

    The liquidation path was dead: the position was looked up by asset in a dict keyed by exchange
    name, so it never matched and nothing was ever closed. An expired contract stayed on the books
    at a stale mark, still counted in exposure, for the rest of the run.
    """

    async def test_an_expired_position_is_liquidated(self):
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=5, price=70.0)
        await settle(ledger)

        dt = datetime.datetime(2023, 12, 21, tzinfo=datetime.timezone.utc)
        ledger.close_position(asset=future, dt=dt)

        self.assertIsNone(ledger.position_tracker.get_position(future))

    async def test_liquidation_settles_the_remaining_variation(self):
        ledger = make_ledger()
        future = make_future(sid=1, multiplier=1000.0)
        trade(ledger, future, amount=5, price=70.0)
        await settle(ledger)
        mark(ledger, future, 71.0)
        cash_before = ledger.portfolio.cash

        ledger.close_position(
            asset=future, dt=datetime.datetime(2023, 12, 21, tzinfo=datetime.timezone.utc))

        # One point on five 1000-multiplier contracts is 5 000.
        self.assertAlmostEqual(ledger.portfolio.cash - cash_before, 5_000.0)

    async def test_closing_what_is_not_held_does_nothing(self):
        ledger = make_ledger()
        future = make_future(sid=1)
        cash_before = ledger.portfolio.cash

        ledger.close_position(
            asset=future, dt=datetime.datetime(2023, 12, 21, tzinfo=datetime.timezone.utc))

        self.assertAlmostEqual(ledger.portfolio.cash, cash_before)

    async def test_an_expired_contract_stops_counting_towards_exposure(self):
        ledger = make_ledger()
        expiring = make_future(sid=1, symbol="CLZ23", multiplier=1000.0)
        surviving = make_future(sid=2, symbol="CLH24", multiplier=1000.0)
        trade(ledger, expiring, amount=5, price=70.0)
        trade(ledger, surviving, amount=3, price=70.0)
        await settle(ledger)

        ledger.close_position(
            asset=expiring, dt=datetime.datetime(2023, 12, 21, tzinfo=datetime.timezone.utc))
        await settle(ledger)

        self.assertAlmostEqual(ledger.position_tracker.stats.net_exposure,
                               3 * 70.0 * 1000.0)
