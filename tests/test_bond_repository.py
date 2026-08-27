"""Persisting bonds and their schedules.

An ingest is re-run whenever the schedule is refreshed, so the two things that matter here are that
everything survives a round trip intact and that re-running adds nothing. A duplicated coupon is
not a cosmetic problem: the simulation pays each row it finds.
"""
import datetime as dt
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.data_sources.demo_bonds import build_demo_bond_universe, demo_exchange

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_bond(name: str = "REPOTEST", isin: str = "REPOTEST0001") -> Bond:
    return Bond(
        id=None, isin=isin, asset_name=name,
        start_date=dt.date(2021, 3, 15), end_date=dt.date(2028, 3, 14),
        first_traded=dt.date(2021, 3, 15), auto_close_date=dt.date(2028, 3, 15),
        face_value=1000.0, maturity_date=dt.date(2028, 3, 15),
        coupon_rate=0.0925, coupon_frequency=4, quote_currency="RUB",
        day_count=DayCount.ACT_ACT, price_quotation=PriceQuotation.PERCENT_OF_FACE,
        is_amortized=True,
    )


class BondPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-bond-repo-")
        self.db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.db_path)
        self.asset_service = get_asset_service(db_path=str(self.db_path))

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self.temp_dir.cleanup()

    async def test_a_bond_round_trips_with_every_term_intact(self):
        original = make_bond()

        [saved] = await self.asset_service.save_bonds([original])
        [reloaded] = await self.asset_service.get_bonds_by_isins([original.isin])

        self.assertIsNotNone(saved.id)
        self.assertEqual(reloaded.asset_name, original.asset_name)
        self.assertAlmostEqual(reloaded.face_value, original.face_value)
        self.assertEqual(reloaded.maturity_date, original.maturity_date)
        self.assertAlmostEqual(reloaded.coupon_rate, original.coupon_rate)
        self.assertEqual(reloaded.coupon_frequency, original.coupon_frequency)
        self.assertEqual(reloaded.day_count, DayCount.ACT_ACT)
        self.assertEqual(reloaded.price_quotation, PriceQuotation.PERCENT_OF_FACE)
        self.assertTrue(reloaded.is_amortized)

    async def test_a_bond_lands_in_the_asset_router(self):
        [saved] = await self.asset_service.save_bonds([make_bond()])

        with sqlite3.connect(self.db_path) as connection:
            asset_type = connection.execute(
                "SELECT asset_type FROM asset_router WHERE id = ?", (saved.id,)).fetchone()[0]

        self.assertEqual(asset_type, AssetType.BOND.value)

    async def test_saving_the_same_bond_twice_keeps_one_row(self):
        original = make_bond()
        [first] = await self.asset_service.save_bonds([original])
        [second] = await self.asset_service.save_bonds([original])

        self.assertEqual(first.id, second.id,
                         "a second ingest must not mint a new asset id and strand the schedule")
        with sqlite3.connect(self.db_path) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM bonds WHERE asset_name = ?",
                (original.asset_name,)).fetchone()[0]
        self.assertEqual(count, 1)

    async def test_events_round_trip_with_every_detail(self):
        [bond] = await self.asset_service.save_bonds([make_bond()])
        events = [
            BondEvent(asset=bond, event_type=BondEventType.COUPON,
                      date=dt.date(2024, 3, 15), value=23.13, currency="RUB",
                      record_date=dt.date(2024, 3, 14),
                      period_start_date=dt.date(2023, 12, 15),
                      face_value=1000.0, value_percent=9.25),
            BondEvent(asset=bond, event_type=BondEventType.AMORTIZATION,
                      date=dt.date(2025, 3, 15), value=250.0, currency="RUB",
                      new_face_value=750.0, initial_face_value=1000.0,
                      amortization_percent=25.0),
            BondEvent(asset=bond, event_type=BondEventType.OFFER,
                      date=dt.date(2026, 3, 15), value=0.0, currency="RUB",
                      offer_type="put", offer_price=100.0,
                      offer_start_date=dt.date(2026, 3, 8),
                      offer_end_date=dt.date(2026, 3, 15), offer_agent="Broker"),
        ]

        await self.asset_service.save_bond_events(events)
        stored = (await self.asset_service.get_bond_events(bonds=[bond]))[bond.id]

        by_type = {event.event_type: event for event in stored}
        self.assertEqual(len(stored), 3)
        coupon = by_type[BondEventType.COUPON]
        self.assertEqual(coupon.record_date, dt.date(2024, 3, 14))
        self.assertEqual(coupon.period_start_date, dt.date(2023, 12, 15))
        self.assertAlmostEqual(coupon.value_percent, 9.25)
        self.assertAlmostEqual(by_type[BondEventType.AMORTIZATION].new_face_value, 750.0)
        offer = by_type[BondEventType.OFFER]
        self.assertEqual(offer.offer_type, "put")
        self.assertEqual(offer.offer_end_date, dt.date(2026, 3, 15))

    async def test_reimporting_a_schedule_does_not_duplicate_it(self):
        [bond] = await self.asset_service.save_bonds([make_bond()])
        events = [BondEvent(asset=bond, event_type=BondEventType.COUPON,
                            date=dt.date(2024, 3, 15), value=23.13, currency="RUB")]

        await self.asset_service.save_bond_events(events)
        await self.asset_service.save_bond_events(events)

        stored = (await self.asset_service.get_bond_events(bonds=[bond]))[bond.id]
        self.assertEqual(len(stored), 1)

    async def test_events_for_an_unsaved_bond_are_refused(self):
        # Storing them against a null asset id would silently detach the schedule.
        unsaved = make_bond(name="NEVERSAVED", isin="NEVERSAVED01")
        event = BondEvent(asset=unsaved, event_type=BondEventType.COUPON,
                          date=dt.date(2024, 3, 15), value=1.0)

        with self.assertRaises(ValueError):
            await self.asset_service.save_bond_events([event])

    async def test_a_bond_is_reachable_as_a_listing(self):
        await self.asset_service.save_exchanges(exchanges=[demo_exchange()])
        await self.asset_service.import_assets(assets_import=build_demo_bond_universe())

        listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="ZLB26", mic="MISX"), asset_type=AssetType.BOND)

        self.assertIsNotNone(listing)
        self.assertIsInstance(listing.asset, Bond)
        self.assertEqual(listing.mic, "MISX")

    async def test_looking_a_bond_up_as_an_equity_finds_nothing(self):
        # Listings are filtered by instrument type, so the same ticker can be a bond on one venue
        # and something else on another without the two colliding.
        await self.asset_service.save_exchanges(exchanges=[demo_exchange()])
        await self.asset_service.import_assets(assets_import=build_demo_bond_universe())

        listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="ZLB26", mic="MISX"), asset_type=AssetType.EQUITY)

        self.assertIsNone(listing)

    async def test_importing_a_universe_attaches_schedules_to_the_stored_bonds(self):
        # The entities handed to import_assets still carry ``id=None``; the events have to end up
        # pointing at the rows that were actually written.
        await self.asset_service.save_exchanges(exchanges=[demo_exchange()])
        assets_import = build_demo_bond_universe(tickers=["ZLA27"])

        await self.asset_service.import_assets(assets_import=assets_import)

        [stored] = await self.asset_service.get_bonds_by_isins([assets_import.bonds[0].isin])
        events = (await self.asset_service.get_bond_events(bonds=[stored]))[stored.id]
        self.assertEqual(len(events), len(assets_import.bond_events))

    async def test_a_bond_appears_in_the_full_asset_map(self):
        [saved] = await self.asset_service.save_bonds([make_bond()])
        assets = await self.asset_service._asset_repository.get_all_assets()
        self.assertIsInstance(assets[saved.id], Bond)


if __name__ == "__main__":
    unittest.main(verbosity=2)
