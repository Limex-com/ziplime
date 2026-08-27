"""Bond behaviour through a whole simulation, not just the ledger.

The ledger tests pin the arithmetic. These pin the wiring: that a backtest resolves a bond by
ticker, prices it against its face value, has its coupons reach cash through the session loop, and
redeems it at maturity -- and that a bond which pays nothing produces exactly the return its price
path implies, with no phantom cash flows.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import io
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.data_sources.demo_bonds import (
    DEMO_BONDS_BY_TICKER, build_demo_bond_bars, build_demo_bond_universe, demo_clean_price,
    demo_exchange,
)
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.finance.commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALGORITHM = str(PROJECT_ROOT / "tests" / "fixtures" / "bond_buy_and_hold.py")
ORDER_VALUE_ALGORITHM = str(PROJECT_ROOT / "tests" / "fixtures" / "bond_order_value.py")
TZ = ZoneInfo("Europe/Moscow")
CALENDAR = "XMOS"
QUANTITY = 100
STARTING_CASH = 1_000_000.0


class BondSimulationTests(unittest.IsolatedAsyncioTestCase):
    """Each case seeds a fresh copy of the asset database and runs one backtest."""

    async def run_backtest(self, ticker: str, start: dt.date, end: dt.date,
                           quantity: int = QUANTITY, algorithm: str = ALGORITHM,
                           algorithm_result=None):
        """Run buy-and-hold over ``ticker`` and return ``(final cash, portfolio value, errors)``.

        ``algorithm_result`` is an optional dict the caller can hand in; whatever the algorithm
        left on its context that the caller names is copied into it before the service is closed.
        """
        with tempfile.TemporaryDirectory(prefix="ziplime-bond-e2e-") as temp_dir:
            db_path = Path(temp_dir) / "assets.sqlite"
            shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
            asset_service = get_asset_service(db_path=str(db_path))
            await asset_service.save_exchanges(exchanges=[demo_exchange()])
            await asset_service.import_assets(assets_import=build_demo_bond_universe())

            listing = await asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="MISX"), asset_type=AssetType.BOND)
            self.assertIsNotNone(listing, f"{ticker} was not stored")

            bundle = self.build_bundle([listing], start, end, asset_service)

            previous = os.environ.get("ZIPLIME_TEST_BOND_TICKER")
            os.environ["ZIPLIME_TEST_BOND_TICKER"] = ticker
            os.environ["ZIPLIME_TEST_BOND_QUANTITY"] = str(quantity)
            try:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    result = await run_simulation(
                        start_date=dt.datetime.combine(start, dt.time.min, tzinfo=TZ),
                        end_date=dt.datetime.combine(end, dt.time.max, tzinfo=TZ),
                        trading_calendar=CALENDAR,
                        emission_rate=dt.timedelta(days=1),
                        total_cash=STARTING_CASH,
                        market_data_source=bundle,
                        custom_data_sources=[],
                        algorithm_file=algorithm,
                        stop_on_error=True,
                        asset_service=asset_service,
                        benchmark_asset_symbol=None,
                        benchmark_returns=None,
                        # No frictions: the tests assert exact cash flows, and a commission or a
                        # slippage model would put a small unknown in front of every one of them.
                        bond_commission=NoCommission(),
                        bond_slippage=NoSlippage(),
                        equity_commission=NoCommission(),
                        max_leverage=1.0,
                        same_bar_execution=True,
                        price_used_in_order_execution="close",
                        print_algo=False,
                    )
            finally:
                if previous is None:
                    os.environ.pop("ZIPLIME_TEST_BOND_TICKER", None)
                else:
                    os.environ["ZIPLIME_TEST_BOND_TICKER"] = previous
                os.environ.pop("ZIPLIME_TEST_BOND_QUANTITY", None)

            algorithm = result.trading_algorithm
            portfolio = algorithm.portfolio
            errors = list(result.errors or [])
            if algorithm_result is not None:
                algorithm_result["position"] = next(
                    (p for p in algorithm._ledger.position_tracker.get_position_list()), None)
                algorithm_result["realism"] = list(getattr(algorithm, "realism", []))
            await asset_service._asset_repository.engine.dispose()
            return portfolio.cash, portfolio.portfolio_value, errors

    @staticmethod
    def build_bundle(listings, start: dt.date, end: dt.date, asset_service) -> DataBundle:
        calendar = get_calendar(CALENDAR)
        sessions = calendar.sessions_in_range(start, end)
        closes = {session.date(): close.to_pydatetime() for session, close in
                  calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz).items()}
        frame = build_demo_bond_bars(listings=listings, sessions=list(closes))
        frame = frame.with_columns(
            pl.col("date").map_elements(
                closes.__getitem__,
                return_dtype=pl.Datetime(time_unit="us", time_zone=str(calendar.tz)))
        ).sort(["sid", "date"])
        indexes = frame.with_row_index().group_by("sid", maintain_order=True).agg([
            pl.col("index").first().alias("start"), pl.col("index").last().alias("end")])
        sid_indexes = {r["sid"]: (r["start"], r["end"] + 1) for r in indexes.iter_rows(named=True)}
        return DataBundle(
            name="bond-e2e", version="1",
            start_date=frame["date"].min(), end_date=frame["date"].max(),
            trading_calendar=calendar, frequency=dt.timedelta(days=1),
            original_frequency=dt.timedelta(days=1), data_type=DataType.MARKET_DATA,
            timestamp=frame["date"].max(), data=frame, sid_indexes=sid_indexes,
            asset_service=asset_service)

    # -- cases ---------------------------------------------------------------------------------

    async def test_a_coupon_bond_earns_exactly_its_schedule(self):
        # One year of a semi-annual 8.5% bond: two coupons of 42.50 on 100 bonds. Nothing else
        # moves cash, so the change in cash after the purchase is the schedule, to the kopeck.
        start, end = dt.date(2023, 6, 1), dt.date(2024, 6, 1)
        cash, _, errors = await self.run_backtest("ZLB26", start, end)

        self.assertEqual(errors, [])
        spec = DEMO_BONDS_BY_TICKER["ZLB26"]
        paid = QUANTITY * _dirty_at_entry(spec, start)
        coupons = QUANTITY * 42.5 * 2
        self.assertAlmostEqual(cash, STARTING_CASH - paid + coupons, places=2)

    async def test_a_zero_coupon_bond_earns_nothing_until_redemption(self):
        # The control. Same window, same quantity, an issue with no schedule: cash must be exactly
        # what was left after the purchase.
        start, end = dt.date(2023, 6, 1), dt.date(2024, 6, 1)
        cash, _, errors = await self.run_backtest("ZLZ26", start, end)

        self.assertEqual(errors, [])
        spec = DEMO_BONDS_BY_TICKER["ZLZ26"]
        paid = QUANTITY * _dirty_at_entry(spec, start)
        self.assertAlmostEqual(cash, STARTING_CASH - paid, places=2)

    async def test_the_coupon_bond_beats_the_zero_by_its_coupons(self):
        # Both issues share an issue date and a maturity, so over the same window the only
        # structural difference between them is the coupon stream.
        start, end = dt.date(2023, 6, 1), dt.date(2024, 6, 1)
        coupon_cash, _, _ = await self.run_backtest("ZLB26", start, end)
        zero_cash, _, _ = await self.run_backtest("ZLZ26", start, end)

        spec_coupon = DEMO_BONDS_BY_TICKER["ZLB26"]
        spec_zero = DEMO_BONDS_BY_TICKER["ZLZ26"]
        price_difference = QUANTITY * (_dirty_at_entry(spec_zero, start)
                                       - _dirty_at_entry(spec_coupon, start))
        self.assertAlmostEqual(coupon_cash - zero_cash - price_difference,
                               QUANTITY * 42.5 * 2, places=2)

    async def test_a_bond_is_redeemed_at_par_at_maturity(self):
        # ZLS25 matures inside this window. After redemption the whole portfolio is cash, and the
        # position is worth nothing because there is no position.
        start, end = dt.date(2024, 6, 3), dt.date(2025, 6, 2)
        cash, value, errors = await self.run_backtest("ZLS25", start, end)

        self.assertEqual(errors, [])
        self.assertAlmostEqual(cash, value, places=2,
                               msg="after redemption the portfolio should be entirely cash")

    async def test_redemption_returns_the_principal_and_the_final_coupons(self):
        start, end = dt.date(2024, 6, 3), dt.date(2025, 6, 2)
        cash, _, _ = await self.run_backtest("ZLS25", start, end)

        spec = DEMO_BONDS_BY_TICKER["ZLS25"]
        paid = QUANTITY * _dirty_at_entry(spec, start)
        # Coupons on 2024-10-05 and 2025-04-05, then 1000 of principal per bond.
        expected = STARTING_CASH - paid + QUANTITY * (45.0 * 2 + 1000.0)
        self.assertAlmostEqual(cash, expected, places=2)

    async def test_an_amortizing_bond_returns_principal_during_the_run(self):
        # One instalment (2025-03-20) falls inside this window, alongside four quarterly coupons
        # whose size drops once the principal does.
        start, end = dt.date(2024, 6, 3), dt.date(2025, 6, 2)
        cash, _, errors = await self.run_backtest("ZLA27", start, end)

        self.assertEqual(errors, [])
        spec = DEMO_BONDS_BY_TICKER["ZLA27"]
        paid = QUANTITY * _dirty_at_entry(spec, start)
        # 750 outstanding until 2025-03-20, then 500. Coupons: 2024-06-20, 09-20, 12-20 and
        # 2025-03-20 on 750, then 2025-06-20 is past the window.
        coupons = 4 * (750.0 * 0.11 / 4)
        expected = STARTING_CASH - paid + QUANTITY * (round(coupons / 4, 2) * 4 + 250.0)
        self.assertAlmostEqual(cash, expected, places=2)

    async def test_the_position_is_valued_against_face_value(self):
        # A hundred bonds near par is about 100 000 roubles of position, not about 10 000. Getting
        # this wrong is invisible in the cash but wrecks every leverage and exposure figure.
        start, end = dt.date(2023, 6, 1), dt.date(2023, 9, 1)
        cash, value, errors = await self.run_backtest("ZLB26", start, end)

        self.assertEqual(errors, [])
        position_value = value - cash
        self.assertGreater(position_value, 90_000.0)
        self.assertLess(position_value, 110_000.0)


class BondOrderSizingTests(BondSimulationTests):
    """Sizing a bond order by money rather than by count."""

    async def test_order_value_sizes_against_the_dirty_price(self):
        # 100 000 roubles of a bond near par is about 100 bonds, not about 1 000. Sizing off the
        # quote -- a percentage of face value -- would buy roughly ten times too many.
        start, end = dt.date(2023, 6, 1), dt.date(2023, 9, 1)
        captured: dict = {}
        os.environ["ZIPLIME_TEST_BOND_VALUE"] = "100000"
        try:
            _, _, errors = await self.run_backtest(
                "ZLB26", start, end, algorithm=ORDER_VALUE_ALGORITHM,
                algorithm_result=captured)
        finally:
            os.environ.pop("ZIPLIME_TEST_BOND_VALUE", None)

        self.assertEqual(errors, [])
        position = captured["position"]
        self.assertIsNotNone(position)
        spec = DEMO_BONDS_BY_TICKER["ZLB26"]
        expected = int(100_000 / _dirty_at_entry(spec, start))
        self.assertEqual(position.amount, expected)
        self.assertLess(position.amount, 120)

    async def test_a_bond_run_reports_its_realism_gaps(self):
        start, end = dt.date(2023, 6, 1), dt.date(2023, 9, 1)
        captured: dict = {}
        await self.run_backtest("ZLB26", start, end, algorithm=ORDER_VALUE_ALGORITHM,
                                algorithm_result=captured)

        codes = " ".join(captured["realism"])
        self.assertIn("NO CREDIT RISK", codes)
        self.assertIn("OFFERS ARE NOT EXERCISED", codes)


class SeededDatabaseTests(unittest.IsolatedAsyncioTestCase):
    """The demo universe as it lands in data/assets.sqlite."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-bond-db-")
        self.db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.db_path)
        self.asset_service = get_asset_service(db_path=str(self.db_path))

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self.temp_dir.cleanup()

    async def test_the_shipped_database_already_carries_the_demo_bonds(self):
        # data/assets.sqlite is seeded, so the examples run without an ingest step.
        with sqlite3.connect(self.db_path) as connection:
            tickers = {row[0] for row in connection.execute(
                "SELECT asset_name FROM bonds")}
        self.assertTrue(set(DEMO_BONDS_BY_TICKER).issubset(tickers),
                        f"missing demo bonds: {set(DEMO_BONDS_BY_TICKER) - tickers}")

    async def test_every_demo_bond_is_tradeable(self):
        for ticker in DEMO_BONDS_BY_TICKER:
            listing = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="MISX"), asset_type=AssetType.BOND)
            self.assertIsNotNone(listing, f"{ticker} has no listing")
            self.assertEqual(listing.asset.asset_name, ticker)

    async def test_schedules_round_trip_through_the_database(self):
        listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="ZLA27", mic="MISX"), asset_type=AssetType.BOND)
        events = (await self.asset_service.get_bond_events(bonds=[listing.asset]))[listing.asset.id]

        kinds = {event.event_type.value for event in events}
        self.assertIn("COUPON", kinds)
        self.assertIn("AMORTIZATION", kinds)
        # Coupons of an amortizing issue shrink as principal is repaid.
        coupons = sorted((e for e in events if e.event_type.value == "COUPON"),
                         key=lambda e: e.date)
        self.assertGreater(coupons[0].value, coupons[-1].value)

    async def test_reseeding_does_not_duplicate_anything(self):
        await self.asset_service.save_exchanges(exchanges=[demo_exchange()])
        await self.asset_service.import_assets(assets_import=build_demo_bond_universe())

        with sqlite3.connect(self.db_path) as connection:
            bonds = connection.execute(
                "SELECT COUNT(*) FROM bonds WHERE asset_name = 'ZLA27'").fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM bond_events WHERE asset_id = "
                "(SELECT id FROM bonds WHERE asset_name = 'ZLA27')").fetchone()[0]
        self.assertEqual(bonds, 1)
        self.assertEqual(events, 20)


def _dirty_at_entry(spec, session: dt.date) -> float:
    """Money one bond costs on the first session of a run, matching what the engine charges."""
    from ziplime.data.data_sources.demo_bonds import build_bond, build_bond_events
    from ziplime.finance.bonds import BondBook

    calendar = get_calendar(CALENDAR)
    first_session = calendar.sessions_in_range(session, session + dt.timedelta(days=10))[0].date()
    bond = build_bond(spec)
    book = BondBook()
    book.add(bond.id, build_bond_events(spec, bond))
    return book.dirty_value(bond, demo_clean_price(spec, first_session), first_session)


if __name__ == "__main__":
    unittest.main(verbosity=2)
