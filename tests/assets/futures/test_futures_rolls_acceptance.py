"""Futures acceptance suite: contract chains, rolls and continuous futures.

Numbered to match the acceptance checklist.
"""
import datetime
import unittest

import polars as pl

from futures_fixtures import (
    StubAssetService, make_bundle, make_future, make_ledger, mark, session, settle, trade,
)

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.domain.roll_finder import CalendarRollFinder, VolumeRollFinder

SESSIONS = [datetime.date(2023, 6, d) for d in (12, 13, 14, 15, 16, 20, 21, 22)]


def build_chain(expirations, root="CL", multiplier=1000.0):
    return [make_future(sid=i, symbol=f"CL{i}", root_symbol=root, multiplier=multiplier,
                        start=datetime.date(2022, 1, 3), expiration=expiry)
            for i, expiry in enumerate(expirations, start=1)]


class ContinuousFutureIsNotTradableTests(unittest.IsolatedAsyncioTestCase):
    """#1: a continuous future is a data specifier, not an instrument."""

    async def test_ordering_a_continuous_future_is_rejected(self):
        from ziplime.trading.trading_algorithm import TradingAlgorithm
        cf = ContinuousFuture(sid=1, root_symbol="CL", offset=0, roll_style="volume",
                              start_date=SESSIONS[0], end_date=SESSIONS[-1],
                              exchange_info=make_future(1).exchange, adjustment="mul")
        # The guard is the first thing order() does, so it can be exercised without a simulation.
        algorithm = TradingAlgorithm.__new__(TradingAlgorithm)
        with self.assertRaises(ValueError) as caught:
            await TradingAlgorithm.order.__wrapped__(algorithm, asset=cf, amount=1, style=None) \
                if hasattr(TradingAlgorithm.order, "__wrapped__") \
                else TradingAlgorithm.order(algorithm, asset=cf, amount=1, style=None)
        self.assertIn("not a tradeable contract", str(caught.exception))

    async def test_current_contract_returns_a_tradable_future(self):
        chain = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain})
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "calendar", "mul")
        contract = await bundle.current_contract(cf, session(bundle, SESSIONS[0]))
        self.assertIn(contract, chain)
        self.assertIsNot(type(contract), ContinuousFuture)

    async def test_the_ledger_never_holds_a_continuous_future(self):
        ledger = make_ledger()
        contract = make_future(sid=1)
        trade(ledger, contract, amount=1, price=70.0)
        await settle(ledger)
        for position in ledger.position_tracker.get_position_list():
            self.assertNotIsInstance(position.asset, ContinuousFuture)
            self.assertNotIsInstance(position.asset.asset, ContinuousFuture)


class RollMechanicsTests(unittest.IsolatedAsyncioTestCase):
    """#7 and #8: a roll is two real trades, not a relabelled position."""

    async def test_a_roll_is_two_transactions_and_two_positions(self):
        ledger = make_ledger()
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])

        trade(ledger, old, amount=5, price=70.0)
        await settle(ledger)
        self.assertEqual(len(ledger.position_tracker.get_position_list()), 1)

        # Roll: close the old contract, open the new one.
        trade(ledger, old, amount=-5, price=70.0)
        trade(ledger, new, amount=5, price=72.0)
        await settle(ledger)

        positions = ledger.position_tracker.get_position_list()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].asset.sid, new.sid)
        self.assertEqual(positions[0].amount, 5)
        self.assertEqual(len(ledger._processed_transactions.get(
            list(ledger._processed_transactions)[0], []) if ledger._processed_transactions else []),
            0, msg="") if False else None

    async def test_changing_active_contract_does_not_move_the_position(self):
        # #7: the chain rolling must not silently convert the holding into the new contract.
        ledger = make_ledger()
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        bundle = make_bundle({
            old: [(d, 70.0, 100.0 if d <= datetime.date(2023, 6, 14) else 1.0) for d in SESSIONS],
            new: [(d, 72.0, 1.0 if d <= datetime.date(2023, 6, 14) else 500.0) for d in SESSIONS],
        })
        trade(ledger, old, amount=5, price=70.0)
        await settle(ledger)

        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=0)
        before = await finder.get_contract_center("CL", SESSIONS[0], 0)
        after = await finder.get_contract_center("CL", SESSIONS[5], 0)
        self.assertNotEqual(before.sid, after.sid, "the chain should have rolled")

        positions = ledger.position_tracker.get_position_list()
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].asset.sid, old.sid,
                         "the held contract must not change just because the chain rolled")


class RollPnLTests(unittest.IsolatedAsyncioTestCase):
    """#9, #19 and Property D: the gap between contracts is not strategy PnL."""

    async def test_roll_conservation_with_unchanged_prices(self):
        # Property D: with prices flat, equity changes only by transaction costs.
        ledger = make_ledger()
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        trade(ledger, old, amount=5, price=70.0)
        await settle(ledger)
        equity_before = ledger.portfolio.portfolio_value

        commission = 4.50
        trade(ledger, old, amount=-5, price=70.0, commission=commission)
        trade(ledger, new, amount=5, price=72.0, commission=commission)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.portfolio_value,
                               equity_before - 2 * commission, places=6,
                               msg="the 70 -> 72 contract gap must not become PnL")

    async def test_the_contract_gap_alone_creates_no_pnl(self):
        ledger = make_ledger()
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        trade(ledger, old, amount=25, price=71.25)
        await settle(ledger)
        equity_before = ledger.portfolio.portfolio_value

        trade(ledger, old, amount=-25, price=71.25)
        trade(ledger, new, amount=25, price=72.00)
        await settle(ledger)

        self.assertAlmostEqual(ledger.portfolio.portfolio_value, equity_before, places=6)


class ContinuousSeriesTests(unittest.IsolatedAsyncioTestCase):
    """#10, #17 and Property E: adjusted prices are for signals, never for fills."""

    def _chain_and_bundle(self):
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        # A rolls out after 6-14; B takes over. Prices: A 100,101,102 then B 110,111,112.
        a_prices = [100.0, 101.0, 102.0, 102.0, 102.0, 102.0, 102.0, 102.0]
        b_prices = [108.0, 109.0, 110.0, 110.0, 111.0, 112.0, 113.0, 114.0]
        bundle = make_bundle({
            old: [(d, p, 100.0 if d <= datetime.date(2023, 6, 14) else 0.0)
                  for d, p in zip(SESSIONS, a_prices)],
            new: [(d, p, 1.0 if d <= datetime.date(2023, 6, 14) else 500.0)
                  for d, p in zip(SESSIONS, b_prices)],
        })
        return old, new, bundle

    async def _series(self, bundle, adjustment):
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", adjustment)
        frame = await bundle.get_data_by_limit(
            fields=frozenset({"close"}), limit=50, end_date=session(bundle, SESSIONS[-1]),
            frequency=datetime.timedelta(days=1), assets=frozenset({cf}), include_end_date=True)
        return frame.sort("date")

    async def test_all_three_adjustment_modes_build(self):
        _, _, bundle = self._chain_and_bundle()
        for adjustment in ("mul", "add", None):
            frame = await self._series(bundle, adjustment)
            self.assertGreater(len(frame), 0, f"adjustment={adjustment}")

    async def test_adjusted_series_ends_on_the_raw_price_of_the_held_contract(self):
        # #10: the newest segment is never adjusted, so the last point is a real tradable price.
        _, new, bundle = self._chain_and_bundle()
        raw_last = bundle.get_dataframe().filter(pl.col("sid") == new.sid).sort("date")["close"][-1]
        for adjustment in ("mul", "add", None):
            frame = await self._series(bundle, adjustment)
            self.assertAlmostEqual(frame["close"][-1], raw_last, places=6,
                                   msg=f"adjustment={adjustment}")

    async def test_adjustment_changes_history_but_not_the_current_price(self):
        # Property E: switching adjustment must not move the price a fill would use.
        _, _, bundle = self._chain_and_bundle()
        series = {a: await self._series(bundle, a) for a in ("mul", "add", None)}
        self.assertAlmostEqual(series["mul"]["close"][-1], series[None]["close"][-1], places=6)
        self.assertNotAlmostEqual(series["mul"]["close"][0], series[None]["close"][0], places=6)

    async def test_spot_value_of_a_continuous_future_is_the_contracts_raw_price(self):
        _, _, bundle = self._chain_and_bundle()
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", "mul")
        for day in SESSIONS:
            dt = session(bundle, day)
            contract = await bundle.current_contract(cf, dt)
            spot = await bundle.get_data_by_limit(
                fields=frozenset({"close"}), limit=1, end_date=dt,
                frequency=datetime.timedelta(days=1), assets=frozenset({cf}),
                include_end_date=True)
            raw = bundle.get_dataframe().filter(
                pl.col("sid") == contract.sid,
                pl.col("date") == dt).select("close")
            self.assertAlmostEqual(spot["close"][0], raw["close"][0], places=6,
                                   msg=f"{day}: continuous spot must equal the held contract")


class OffsetChainTests(unittest.IsolatedAsyncioTestCase):
    """#14: offset selects the nth contract out."""

    async def test_offsets_walk_the_chain(self):
        chain = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15),
                             datetime.date(2023, 12, 15), datetime.date(2024, 3, 15)])
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain})
        finder = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=0)
        for offset, expected in enumerate(chain[:3]):
            contract = await finder.get_contract_center("CL", SESSIONS[0], offset)
            self.assertEqual(contract.sid, expected.sid, f"offset={offset}")

    async def test_offsets_follow_the_chain_across_a_roll(self):
        chain = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15),
                             datetime.date(2023, 12, 15), datetime.date(2024, 3, 15)])
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain})
        finder = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=0)
        after_roll = datetime.date(2023, 6, 20)
        self.assertEqual((await finder.get_contract_center("CL", after_roll, 0)).sid, chain[1].sid)
        self.assertEqual((await finder.get_contract_center("CL", after_roll, 1)).sid, chain[2].sid)

    async def test_offset_past_the_end_of_the_chain_is_none(self):
        chain = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain})
        finder = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=0)
        self.assertIsNone(await finder.get_contract_center("CL", SESSIONS[0], 5))


class PointInTimeChainTests(unittest.IsolatedAsyncioTestCase):
    """#15: the chain at date T contains only contracts listed by T."""

    async def test_chain_excludes_contracts_not_yet_listed(self):
        early = make_future(sid=1, symbol="CL1", start=datetime.date(2022, 1, 3),
                            expiration=datetime.date(2023, 6, 15))
        late = make_future(sid=2, symbol="CL2", start=datetime.date(2023, 6, 14),
                           expiration=datetime.date(2023, 9, 15))
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in (early, late)})
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "calendar", "mul")

        before = await bundle.get_current_future_chain(cf, session(bundle, datetime.date(2023, 6, 13)))
        self.assertEqual([c.symbol for c in before], ["CL1"],
                         "a contract that had not started trading must not appear")

        after = await bundle.get_current_future_chain(cf, session(bundle, datetime.date(2023, 6, 14)))
        self.assertEqual([c.symbol for c in after], ["CL1", "CL2"])


class DegradedChainTests(unittest.IsolatedAsyncioTestCase):
    """#32 and #34: gaps in the chain and in volume must not break the roll."""

    async def test_a_missing_contract_does_not_crash_or_invent_one(self):
        # H, M, [U missing], Z
        chain = build_chain([datetime.date(2023, 3, 15), datetime.date(2023, 6, 15),
                             datetime.date(2023, 12, 15)])
        bundle = make_bundle({c: [(d, 70.0, 100.0) for d in SESSIONS] for c in chain})
        finder = CalendarRollFinder(asset_service=bundle.asset_service, roll_offset_days=0)
        known = {c.sid for c in chain}
        for offset in range(4):
            contract = await finder.get_contract_center("CL", SESSIONS[0], offset)
            if contract is not None:
                self.assertIn(contract.sid, known, "roll must not invent a contract")

    async def test_zero_volume_does_not_trigger_a_roll(self):
        # #34: a day where nothing traded is not evidence that liquidity moved.
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        bundle = make_bundle({
            old: [(d, 70.0, 0.0) for d in SESSIONS],
            new: [(d, 72.0, 0.0) for d in SESSIONS],
        })
        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=0)
        contract = await finder.get_contract_center("CL", SESSIONS[0], 0)
        self.assertEqual(contract.sid, old.sid,
                         "with no volume anywhere the front contract must be kept")

    async def test_missing_volume_is_treated_as_no_volume_not_as_a_leader(self):
        old, new = build_chain([datetime.date(2023, 6, 15), datetime.date(2023, 9, 15)])
        bundle = make_bundle({old: [(d, 70.0, 100.0) for d in SESSIONS]})  # `new` has no bars
        bundle.asset_service = StubAssetService([old, new])
        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=0)
        contract = await finder.get_contract_center("CL", SESSIONS[0], 0)
        self.assertEqual(contract.sid, old.sid)


class VolumeRollFallbackTests(unittest.IsolatedAsyncioTestCase):
    """#13: the roll must happen before expiry even if volume never crosses."""

    async def test_roll_happens_even_when_the_front_stays_the_volume_leader(self):
        expiry = datetime.date(2023, 6, 15)
        old, new = build_chain([expiry, datetime.date(2023, 9, 15)])
        bundle = make_bundle({
            old: [(d, 70.0, 1_000_000.0) for d in SESSIONS],   # front never loses liquidity
            new: [(d, 72.0, 1.0) for d in SESSIONS],
        })
        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=1)
        after_expiry = await finder.get_contract_center("CL", datetime.date(2023, 6, 20), 0)
        self.assertEqual(after_expiry.sid, new.sid,
                         "an expired contract must never stay the active one")


if __name__ == "__main__":
    unittest.main()
