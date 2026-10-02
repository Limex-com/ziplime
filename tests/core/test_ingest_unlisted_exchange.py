"""A listing on an exchange the asset source did not report up front.

Yahoo maps the venues it knows to MICs and returns the rest under its own codes (NGM, SHZ, ...).
Those listings used to be written with no row in ``exchanges``, read back with ``exchange = None``,
and fail every bar of a Hugging Face mount with ``'NoneType' object has no attribute 'mic'``.
"""
import datetime
import tempfile
import unittest
from pathlib import Path

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime.data.data_sources.asset_data_source import AssetDataSource

START = datetime.datetime(1900, 1, 1, tzinfo=datetime.timezone.utc)
END = datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc)
XNYS = ExchangeInfo(mic="XNYS", name="New York Stock Exchange",
                    canonical_name="New York Stock Exchange", country_code="US")


class _Source(AssetDataSource):
    """Reports XNYS, then lists one equity there and one on a venue it never reported."""

    async def get_exchanges(self, **kwargs):
        return [XNYS]

    async def get_assets(self, exchanges, **kwargs):
        usd = Currency(asset_name="USD", id=None, start_date=START, end_date=END,
                       auto_close_date=END, first_traded=START, isin=None)
        listings, equities = [], []
        for symbol, exchange in (("AAPL", XNYS),
                                 ("VOLV-B.ST", ExchangeInfo(mic="NGM", name="NGM",
                                                            canonical_name="NGM",
                                                            country_code="US"))):
            equity = Equity(asset_name=symbol, id=None, start_date=START, end_date=END,
                            auto_close_date=END, first_traded=START, isin="")
            equities.append(equity)
            listings.append(ExchangeAsset(
                sid=None, symbol=symbol, exchange=exchange, start_date=START, end_date=END,
                auto_close_date=END, first_traded=START, asset=equity, quote=usd,
                external_id=""))
        return AssetsImport(currencies=[usd], equities=equities, exchange_assets=listings)


class UnlistedExchangeTests(unittest.IsolatedAsyncioTestCase):

    async def test_listing_on_an_unreported_exchange_keeps_its_exchange(self):
        with tempfile.TemporaryDirectory() as directory:
            service = get_asset_service(db_path=str(Path(directory) / "assets.sqlite"))
            try:
                await ingest_assets(asset_service=service, asset_data_source=_Source())

                aapl, volvo = await service.get_exchange_assets_by_symbols(
                    symbols=[AssetSymbol(symbol="AAPL", mic=None),
                             AssetSymbol(symbol="VOLV-B.ST", mic=None)],
                    asset_type=AssetType.EQUITY)
            finally:
                await service._asset_repository.engine.dispose()

        self.assertEqual(aapl.mic, "XNYS")
        self.assertIsNotNone(volvo, "the listing on the unreported venue was not stored")
        self.assertIsNotNone(volvo.exchange, "the listing read back with no exchange")
        self.assertEqual(volvo.mic, "NGM")


if __name__ == "__main__":
    unittest.main()
