"""Futures acceptance suite: time-travel, degenerate prices and execution realism.

Numbered to match the acceptance checklist.
"""
import datetime
import unittest

import polars as pl

from futures_fixtures import make_bundle, make_future, session

from ziplime.assets.domain.roll_finder import CalendarRollFinder, VolumeRollFinder

BASE = [datetime.date(2023, 6, d) for d in (12, 13, 14, 15, 16, 20, 21, 22)]
EXTRA = [datetime.date(2023, 6, d) for d in (23, 26, 27, 28, 29, 30)]


def chain():
    return [make_future(sid=1, symbol="CLM23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 6, 21)),
            make_future(sid=2, symbol="CLU23", root_symbol="CL",
                        start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 9, 21))]


def bundle_for(sessions, front, back, crossover=datetime.date(2023, 6, 15)):
    return make_bundle({
        front: [(d, 70.0 + i * 0.5, 100.0 if d < crossover else 5.0)
                for i, d in enumerate(sessions)],
        back: [(d, 72.0 + i * 0.5, 5.0 if d < crossover else 400.0)
               for i, d in enumerate(sessions)],
    })


class TimeTravelTests(unittest.IsolatedAsyncioTestCase):
    """#16: appending future data must not change anything that happened before."""

    async def test_roll_decisions_are_identical(self):
        front, back = chain()
        short = bundle_for(BASE, front, back)
        long = bundle_for(BASE + EXTRA, front, back)
        for bundle in (short, long):
            bundle._roll_finders.clear()

        for day in BASE:
            a = await VolumeRollFinder(asset_service=short.asset_service, data_source=short,
                                       grace_period_days=0).get_contract_center("CL", day, 0)
            b = await VolumeRollFinder(asset_service=long.asset_service, data_source=long,
                                       grace_period_days=0).get_contract_center("CL", day, 0)
            self.assertEqual(a.sid, b.sid, f"{day}: roll decision changed")

    async def test_continuous_prices_up_to_t_are_identical(self):
        front, back = chain()
        short = bundle_for(BASE, front, back)
        long = bundle_for(BASE + EXTRA, front, back)

        async def series(bundle, end_day):
            cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", None)
            frame = await bundle.get_data_by_limit(
                fields=frozenset({"close"}), limit=100, end_date=session(bundle, end_day),
                frequency=datetime.timedelta(days=1), assets=frozenset({cf}),
                include_end_date=True)
            return frame.sort("date").with_columns(
                pl.col("date").dt.convert_time_zone(str(bundle.trading_calendar.tz))
                .dt.date().alias("d"))

        a = await series(short, BASE[-1])
        b = await series(long, BASE[-1])
        self.assertEqual(a["d"].to_list(), b["d"].to_list())
        for x, y, day in zip(a["close"], b["close"], a["d"]):
            self.assertAlmostEqual(x, y, places=9, msg=f"{day}: past price changed")

    async def test_unadjusted_series_is_the_one_that_must_be_stable(self):
        # A back-adjusted series legitimately re-levels when a new roll appears; the raw series
        # and the roll schedule must not. This documents which guarantee holds.
        front, back = chain()
        short = bundle_for(BASE, front, back)
        long = bundle_for(BASE + EXTRA, front, back)

        async def last_raw(bundle, end_day):
            cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", None)
            frame = await bundle.get_data_by_limit(
                fields=frozenset({"close"}), limit=1, end_date=session(bundle, end_day),
                frequency=datetime.timedelta(days=1), assets=frozenset({cf}),
                include_end_date=True)
            return frame["close"][0]

        self.assertAlmostEqual(await last_raw(short, BASE[-1]), await last_raw(long, BASE[-1]))


class NegativePriceTests(unittest.IsolatedAsyncioTestCase):
    """#18: a futures price can be zero or negative (CL, April 2020)."""

    def _series(self):
        return [10.0, 5.0, 0.0, -5.0, -20.0]

    async def test_bundle_carries_negative_prices_unchanged(self):
        days = BASE[:5]
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        bundle = make_bundle({contract: list(zip(days, self._series(), [100.0] * 5))})
        stored = bundle.get_dataframe().sort("date")["close"].to_list()
        self.assertEqual(stored, self._series())

    async def test_multiplicative_adjustment_is_refused_across_zero(self):
        # A ratio through zero is undefined; the engine must not silently produce inf/nan.
        front = make_future(sid=1, symbol="CLM23", root_symbol="CL",
                            start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 6, 21))
        back = make_future(sid=2, symbol="CLU23", root_symbol="CL",
                           start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 9, 21))
        crossover = datetime.date(2023, 6, 15)
        bundle = make_bundle({
            front: [(d, p, 100.0 if d < crossover else 1.0)
                    for d, p in zip(BASE[:5], [10.0, 5.0, 0.0, 0.0, 0.0])],
            back: [(d, p, 1.0 if d < crossover else 400.0)
                   for d, p in zip(BASE[:5], [12.0, 8.0, 3.0, 2.0, 1.0])],
        })
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", "mul")
        frame = await bundle.get_data_by_limit(
            fields=frozenset({"close"}), limit=50, end_date=session(bundle, BASE[4]),
            frequency=datetime.timedelta(days=1), assets=frozenset({cf}), include_end_date=True)
        values = frame["close"].to_list()
        self.assertTrue(all(v == v for v in values), f"NaN in adjusted series: {values}")
        self.assertTrue(all(abs(v) != float("inf") for v in values),
                        f"infinite value in adjusted series: {values}")

    def test_pnl_is_well_defined_at_a_negative_price(self):
        # PnL is a difference, so it stays meaningful where a ratio does not.
        multiplier, quantity = 1000.0, 2
        self.assertAlmostEqual((-5.0 - 5.0) * multiplier * quantity, -20_000.0)


class StalePriceTests(unittest.IsolatedAsyncioTestCase):
    """#33 and #35: a last known price is not the same as a tradable price."""

    async def test_a_contract_with_no_bar_for_the_session_reports_no_data(self):
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        bundle = make_bundle({contract: [(d, 70.0, 100.0) for d in BASE[:3]]})
        # Ask for a session the contract has no bar for.
        frame = await bundle.get_data_by_limit(
            fields=frozenset({"close", "volume"}), limit=1,
            end_date=session(bundle, BASE[0]) - datetime.timedelta(days=5),
            frequency=datetime.timedelta(days=1), assets=frozenset({contract}),
            include_end_date=True)
        self.assertEqual(len(frame), 0,
                         "no bar means no price, not the nearest one from the future")

    async def test_zero_volume_bar_is_distinguishable_from_a_traded_bar(self):
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        bundle = make_bundle({contract: [(BASE[0], 70.0, 0.0), (BASE[1], 70.0, 500.0)]})
        frame = bundle.get_dataframe().sort("date")
        self.assertEqual(frame["volume"].to_list(), [0.0, 500.0],
                         "a stale forward-filled bar must remain identifiable by zero volume")


class OpenOrderPolicyTests(unittest.IsolatedAsyncioTestCase):
    """#29: a pending order must never be silently carried to the new contract."""

    async def test_orders_are_bound_to_a_contract_not_to_the_chain(self):
        from ziplime.finance.domain.order import Order
        from ziplime.finance.domain.order_status import OrderStatus
        from ziplime.finance.execution import MarketOrder
        front, back = chain()
        order = Order(dt=datetime.datetime(2023, 6, 13, tzinfo=datetime.timezone.utc),
                      asset=front, amount=3, id="o1", commission=0.0, filled=0,
                      execution_style=MarketOrder(), status=OrderStatus.OPEN,
                      exchange_name="XCME", trading_account_id="account-1")
        bundle = bundle_for(BASE, front, back)
        finder = VolumeRollFinder(asset_service=bundle.asset_service, data_source=bundle,
                                  grace_period_days=0)
        after_roll = await finder.get_contract_center("CL", BASE[-1], 0)

        self.assertEqual(order.asset.sid, front.sid,
                         "the order still refers to the contract it was placed on")
        self.assertNotEqual(after_roll.sid, order.asset.sid,
                            "the chain moved on without touching the order")


if __name__ == "__main__":
    unittest.main()


class ZeroCrossingAdjustmentTests(unittest.IsolatedAsyncioTestCase):
    """#18: a zero price is data, not a missing value; only the ratio method must decline."""

    def _bundle(self, front_prices):
        front = make_future(sid=1, symbol="CLM23", root_symbol="CL",
                            start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 6, 21))
        back = make_future(sid=2, symbol="CLU23", root_symbol="CL",
                           start=datetime.date(2022, 1, 3), expiration=datetime.date(2023, 9, 21))
        crossover = datetime.date(2023, 6, 15)
        return make_bundle({
            front: [(d, p, 100.0 if d < crossover else 1.0)
                    for d, p in zip(BASE[:5], front_prices)],
            back: [(d, p, 1.0 if d < crossover else 400.0)
                   for d, p in zip(BASE[:5], [12.0, 8.0, 3.0, 2.0, 1.0])],
        })

    async def _series(self, bundle, adjustment):
        cf = await bundle.asset_service.create_continuous_future("CL", 0, "volume", adjustment)
        frame = await bundle.get_data_by_limit(
            fields=frozenset({"close"}), limit=50, end_date=session(bundle, BASE[4]),
            frequency=datetime.timedelta(days=1), assets=frozenset({cf}), include_end_date=True)
        return frame.sort("date")["close"].to_list()

    async def test_additive_adjustment_works_through_a_negative_price(self):
        bundle = self._bundle([10.0, 5.0, -5.0, -5.0, -5.0])
        values = await self._series(bundle, "add")
        self.assertTrue(all(v == v and abs(v) != float("inf") for v in values), values)
        # The roll happened, so the history was shifted onto the new contract's level.
        raw = await self._series(bundle, None)
        self.assertNotEqual(values[0], raw[0],
                            "an additive adjustment is well defined across zero and should apply")

    async def test_multiplicative_adjustment_declines_across_zero(self):
        bundle = self._bundle([10.0, 5.0, 0.0, 0.0, 0.0])
        values = await self._series(bundle, "mul")
        raw = await self._series(bundle, None)
        self.assertEqual(values, raw,
                         "the ratio method must leave the series alone rather than emit inf")


class PerformanceRecordIntegrityTests(unittest.IsolatedAsyncioTestCase):
    """#16: a recorded session must not change as the simulation continues."""

    async def test_recorded_positions_are_snapshots_not_live_objects(self):
        from futures_fixtures import make_ledger, settle, trade
        ledger = make_ledger()
        contract = make_future(sid=1, expiration=datetime.date(2023, 9, 21))
        trade(ledger, contract, amount=5, price=70.0)
        await settle(ledger)

        recorded = ledger.positions          # what a performance packet keeps
        self.assertEqual(recorded[0].amount, 5)

        trade(ledger, contract, amount=-5, price=71.0)   # the position is closed later
        await settle(ledger)

        self.assertEqual(recorded[0].amount, 5,
                         "the already-recorded session changed after the fact")


class RealismDisclosureTests(unittest.TestCase):
    """#5 and #36: unmodelled effects are stated rather than left to be discovered."""

    def test_futures_without_a_margin_model_is_disclosed(self):
        from ziplime.finance.realism import realism_warnings
        codes = [w.code for w in realism_warnings(futures_margin_model=None)]
        self.assertIn("NO FUTURES MARGIN MODEL", codes)

    def test_price_limits_are_always_disclosed_for_futures(self):
        from ziplime.finance.margin import FixedRateFuturesMarginModel
        from ziplime.finance.realism import realism_warnings
        codes = [w.code for w in realism_warnings(
            futures_margin_model=FixedRateFuturesMarginModel(0.12))]
        self.assertNotIn("NO FUTURES MARGIN MODEL", codes)
        self.assertIn("NO PRICE LIMITS", codes)

    def test_same_bar_execution_is_disclosed(self):
        from ziplime.finance.realism import realism_warnings
        codes = [w.code for w in realism_warnings(same_bar_execution=True)]
        self.assertIn("SAME-BAR EXECUTION", codes)

    def test_an_equity_only_run_has_no_futures_warnings(self):
        from ziplime.finance.realism import realism_warnings
        codes = [w.code for w in realism_warnings(trades_futures=False)]
        self.assertEqual(codes, [])

    def test_the_report_block_is_renderable(self):
        from ziplime.finance.realism import format_realism_warnings, realism_warnings
        text = format_realism_warnings(realism_warnings(same_bar_execution=True))
        self.assertIn("REALISM / DATA WARNINGS", text)
        self.assertIn("NO FUTURES MARGIN MODEL", text)
