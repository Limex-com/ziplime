"""A named universe resolves to listings a strategy can actually trade.

``symbols_universe`` answers with memberships, and a membership holds the
*issuer*: the asset DB stores it under the company's name -- "Applied
Materials, Inc." where a strategy would write AMAT -- with no symbol, no venue
and no sid. So the composition could be read and nothing in it could be priced
or ordered. ``universe_symbols`` closes that, and these cover it.
"""
import datetime as dt
import shutil
import tempfile
import unittest
from pathlib import Path

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.core.ingest_data import get_asset_service

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UNIVERSE = "Q500US"
WHEN = dt.date(2024, 1, 2)


class UniverseListingsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-universe-")
        db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        self.asset_service = get_asset_service(db_path=str(db_path))

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self.temp_dir.cleanup()

    async def test_the_members_come_back_as_tradeable_listings(self):
        listings = await self.asset_service.get_universe_symbols(name=UNIVERSE, dt=WHEN)

        self.assertTrue(listings, f"{UNIVERSE} has no listings in the test asset DB")
        for listing in listings:
            self.assertIsInstance(listing, ExchangeAsset)
            self.assertIsNotNone(listing.sid)
            self.assertTrue(listing.symbol)
            self.assertTrue(listing.mic)

    async def test_there_is_one_listing_per_venue_the_company_trades_on(self):
        """The US universes carry the Moscow listings of American companies too."""
        listings = await self.asset_service.get_universe_symbols(name=UNIVERSE, dt=WHEN)
        venues = {listing.mic for listing in listings}

        self.assertIn("MISX", venues, "the stored universe holds the -RM listings")
        self.assertTrue(venues - {"MISX"}, "and the American ones")

    async def test_a_venue_can_be_pinned(self):
        pinned = await self.asset_service.get_universe_symbols(
            name=UNIVERSE, dt=WHEN, mic="XNYS")

        self.assertTrue(pinned)
        self.assertEqual({listing.mic for listing in pinned}, {"XNYS"})

    async def test_the_composition_is_the_one_of_that_day(self):
        """A membership that has not started yet is not in the answer."""
        early = await self.asset_service.get_universe_symbols(
            name=UNIVERSE, dt=dt.date(1995, 1, 3))
        late = await self.asset_service.get_universe_symbols(name=UNIVERSE, dt=WHEN)

        self.assertLess(len(early), len(late))

    async def test_listings_are_ordered_by_symbol(self):
        listings = await self.asset_service.get_universe_symbols(name=UNIVERSE, dt=WHEN)

        symbols = [listing.symbol for listing in listings]
        self.assertEqual(symbols, sorted(symbols))

    async def test_an_unknown_universe_is_empty_rather_than_an_error(self):
        self.assertEqual(
            await self.asset_service.get_universe_symbols(name="SPX", dt=WHEN), [])


if __name__ == "__main__":
    unittest.main()
