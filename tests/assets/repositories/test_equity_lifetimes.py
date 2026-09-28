import datetime
import unittest

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from ziplime.assets.models.asset_router import AssetRouter
from ziplime.assets.models.equity_model import EquityModel
from ziplime.assets.models.exchange_asset_model import ExchangeAssetModel
from ziplime.assets.models.exchange_info_model import ExchangeInfoModel
from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository
from ziplime.core.db.base_model import BaseModel


class EquityLifetimesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.addAsyncCleanup(self.engine.dispose)
        async with self.engine.begin() as connection:
            await connection.run_sync(BaseModel.metadata.create_all)
        self.repository = SqlAlchemyAssetRepository.__new__(SqlAlchemyAssetRepository)
        self.repository.session_maker = async_sessionmaker(self.engine, expire_on_commit=False)

    async def test_compute_lifetimes_uses_listing_dates_and_filters_equities_by_country(self):
        first = datetime.date(2020, 1, 1)
        last = datetime.date(2030, 1, 1)
        listed = datetime.date(2023, 3, 1)
        delisted = datetime.date(2025, 6, 1)
        async with self.repository.session_maker() as session:
            session.add_all([
                AssetRouter(id=1, asset_type="equity"),
                AssetRouter(id=2, asset_type="currency"),
                EquityModel(id=1, isin=None, asset_name="TEST", start_date=first, end_date=last,
                            first_traded=first, auto_close_date=last),
                ExchangeInfoModel(mic="XUSA", name="US", canonical_name="US", country_code="US"),
                ExchangeInfoModel(mic="XGBR", name="GB", canonical_name="GB", country_code="GB"),
                *[
                    ExchangeAssetModel(
                        sid=sid, asset_id=asset_id, quote_id=2, mic=mic, symbol=f"LIST{sid}",
                        start_date=listed, end_date=delisted, first_traded=listed,
                        auto_close_date=delisted, external_id=f"listing-{sid}")
                    for sid, asset_id, mic in ((11, 1, "XUSA"), (12, 1, "XGBR"), (13, 2, "XUSA"))
                ],
            ])
            await session.commit()

        lifetimes = await self.repository._compute_lifetimes(frozenset({"US"}))

        self.assertEqual(lifetimes.sid.tolist(), [11])
        self.assertEqual(lifetimes.start.tolist(), [
            int(datetime.datetime.combine(listed, datetime.time(), tzinfo=datetime.UTC).timestamp())])
        self.assertEqual(lifetimes.end.tolist(), [
            int(datetime.datetime.combine(delisted, datetime.time(), tzinfo=datetime.UTC).timestamp())])
        both = await self.repository._compute_lifetimes(frozenset({"US", "GB"}))
        self.assertEqual(set(both.sid), {11, 12})

    async def test_compute_lifetimes_returns_empty_arrays_when_country_has_no_equities(self):
        lifetimes = await self.repository._compute_lifetimes(frozenset({"US"}))

        self.assertEqual(lifetimes.sid.tolist(), [])
        self.assertEqual(lifetimes.start.tolist(), [])
        self.assertEqual(lifetimes.end.tolist(), [])
