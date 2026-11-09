import datetime
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.split import Split
from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.adjustments_service import AdjustmentsService
from ziplime.utils.calendar_utils import get_calendar


@pytest.fixture
def sample_market_bundle():
    calendar = get_calendar("XNYS")
    start = datetime.datetime(2024, 1, 2, 16, 0, tzinfo=calendar.tz)
    end = datetime.datetime(2024, 1, 5, 16, 0, tzinfo=calendar.tz)
    dates = [
        datetime.datetime(2024, 1, 2, 16, 0, tzinfo=calendar.tz),
        datetime.datetime(2024, 1, 3, 16, 0, tzinfo=calendar.tz),
        datetime.datetime(2024, 1, 4, 16, 0, tzinfo=calendar.tz),
        datetime.datetime(2024, 1, 5, 16, 0, tzinfo=calendar.tz),
    ]

    rows = []
    for d in dates:
        rows.append({
            "date": d,
            "sid": 1,
            "symbol": "AAPL",
            "mic": "XNGS",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 100.0,
            "price": 100.0,
            "volume": 1000.0,
        })
    df = pl.DataFrame(rows).sort(["sid", "date"])

    bundle = DataBundle(
        name="test_adj",
        version="v1",
        start_date=start,
        end_date=end,
        trading_calendar=calendar,
        frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1),
        data_type=DataType.MARKET_DATA,
        timestamp=end,
        data=df,
    )
    return bundle


@pytest.mark.asyncio
async def test_adjust_data_bundle_with_split_and_dividend(sample_market_bundle):
    asset_service = MagicMock()
    asset = Asset(
        id=10,
        isin="US0378331005",
        asset_name="Apple Inc",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
    )
    quote = Currency(
        id=1,
        isin=None,
        asset_name="USD",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
    )
    from ziplime.assets.entities.exchange_info import ExchangeInfo
    exchange = ExchangeInfo(
        mic="XNGS",
        name="NASDAQ",
        canonical_name="NASDAQ",
        country_code="US",
    )
    exchange_asset = ExchangeAsset(
        sid=1,
        symbol="AAPL",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
        external_id="AAPL-XNGS",
        exchange=exchange,
        asset=asset,
        quote=quote,
    )

    asset_service.get_exchange_assets_by_sids = AsyncMock(return_value=[exchange_asset])

    # 2-for-1 split on Jan 4: ratio = 0.5 (old / new).
    # Bars strictly before Jan 4 should have price * 0.5 and volume / 0.5.
    split = Split(asset=asset, ratio=0.5, effective_date=datetime.date(2024, 1, 4))
    asset_service.get_splits_by_assets_and_effective_date_between = AsyncMock(return_value=[split])

    # Dividend on Jan 5: amount = 10.0. Preceding close (Jan 4) is 100.0.
    # Factor is (100 - 10) / 100 = 0.9.
    # Bars strictly before Jan 5 should have price * 0.9.
    div = DividendPayout(
        asset=asset,
        amount=10.0,
        pay_date=datetime.date(2024, 1, 6),
        declared_date=datetime.date(2024, 1, 1),
        record_date=datetime.date(2024, 1, 5),
        ex_date=datetime.date(2024, 1, 5),
        currency=quote,
    )
    asset_service.get_dividends_by_assets_and_ex_date_between = AsyncMock(return_value=[div])

    service = AdjustmentsService(asset_service=asset_service)
    adjusted_df = await service.adjust_data_bundle(sample_market_bundle)

    assert "close_adjusted" in adjusted_df.columns
    assert "volume_adjusted" in adjusted_df.columns

    # Jan 2 is before split (factor 0.5) and before div (factor 0.9) -> combined price factor = 0.45
    # Volume factor = 1 / 0.5 = 2.0 -> volume = 2000.0
    jan2_row = adjusted_df.filter(pl.col("date") == sample_market_bundle.data["date"][0])
    assert pytest.approx(jan2_row["close_adjusted"][0]) == 45.0
    assert pytest.approx(jan2_row["volume_adjusted"][0]) == 2000.0

    # Jan 5 (latest bar) is after all events -> unadjusted (factor 1.0)
    jan5_row = adjusted_df.filter(pl.col("date") == sample_market_bundle.data["date"][3])
    assert pytest.approx(jan5_row["close_adjusted"][0]) == 100.0
    assert pytest.approx(jan5_row["volume_adjusted"][0]) == 1000.0


@pytest.mark.asyncio
async def test_adjust_bundle_high_level(sample_market_bundle):
    asset_service = MagicMock()
    service = AdjustmentsService(asset_service=asset_service)

    bundle_service = MagicMock()
    source_storage = MagicMock()
    bundle_service.load_bundle_definition = AsyncMock(return_value=(sample_market_bundle, source_storage))
    bundle_service.load_bundle = AsyncMock(return_value=(sample_market_bundle, []))
    bundle_service.register_bundle = AsyncMock()
    bundle_service.store_bundle = AsyncMock()

    # Empty adjustments
    asset_service.get_exchange_assets_by_sids = AsyncMock(return_value=[])
    asset_service.get_dividends_by_assets_and_ex_date_between = AsyncMock(return_value=[])
    asset_service.get_splits_by_assets_and_effective_date_between = AsyncMock(return_value=[])

    res = await service.adjust_bundle(
        bundle_service=bundle_service,
        bundle_name="test_adj",
        save=True,
    )
    assert "close_adjusted" in res.columns
    bundle_service.register_bundle.assert_awaited_once()
    bundle_service.store_bundle.assert_awaited_once()


@pytest.mark.asyncio
async def test_adjust_data_bundle_empty_and_non_market_data(sample_market_bundle):
    asset_service = MagicMock()
    service = AdjustmentsService(asset_service=asset_service)

    # Empty data
    empty_bundle = DataBundle(
        name="empty",
        version="v1",
        start_date=sample_market_bundle.start_date,
        end_date=sample_market_bundle.end_date,
        trading_calendar=sample_market_bundle.trading_calendar,
        frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1),
        data_type=DataType.MARKET_DATA,
        timestamp=sample_market_bundle.timestamp,
        data=pl.DataFrame(),
    )
    res = await service.adjust_data_bundle(empty_bundle)
    assert res.is_empty()

    # Non-market data
    custom_bundle = DataBundle(
        name="custom",
        version="v1",
        start_date=sample_market_bundle.start_date,
        end_date=sample_market_bundle.end_date,
        trading_calendar=sample_market_bundle.trading_calendar,
        frequency=datetime.timedelta(days=1),
        original_frequency=datetime.timedelta(days=1),
        data_type=DataType.CUSTOM,
        timestamp=sample_market_bundle.timestamp,
        data=sample_market_bundle.data,
    )
    with pytest.raises(ValueError, match="is not MARKET_DATA bundle"):
        await service.adjust_data_bundle(custom_bundle)


@pytest.mark.asyncio
async def test_adjust_bundle_streaming(sample_market_bundle):
    asset_service = MagicMock()
    asset = Asset(
        id=10,
        isin="US0378331005",
        asset_name="Apple Inc",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
    )
    quote = Asset(
        id=1,
        isin=None,
        asset_name="USD",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
    )
    from ziplime.assets.entities.exchange_info import ExchangeInfo
    exchange = ExchangeInfo(
        mic="XNGS",
        name="NASDAQ",
        canonical_name="NASDAQ",
        country_code="US",
    )
    exchange_asset = ExchangeAsset(
        sid=1,
        symbol="AAPL",
        start_date=datetime.date(2020, 1, 1),
        end_date=datetime.date(2030, 1, 1),
        first_traded=datetime.date(2020, 1, 1),
        auto_close_date=datetime.date(2030, 1, 1),
        external_id="AAPL-XNGS",
        exchange=exchange,
        asset=asset,
        quote=quote,
    )
    asset_service.get_exchange_assets_by_sids = AsyncMock(return_value=[exchange_asset])
    asset_service.get_splits_by_assets_and_effective_date_between = AsyncMock(return_value=[])
    asset_service.get_dividends_by_assets_and_ex_date_between = AsyncMock(return_value=[])

    bundle_service = MagicMock()
    source_storage = MagicMock()
    bundle_service.load_bundle_definition = AsyncMock(return_value=(sample_market_bundle, source_storage))
    bundle_service.get_bundle_sids = AsyncMock(return_value=[1])

    async def batch_gen(*args, **kwargs):
        yield sample_market_bundle.data

    bundle_service.iter_bundle_batches = batch_gen
    bundle_service.register_bundle = AsyncMock()
    bundle_service.store_bundle = AsyncMock()

    service = AdjustmentsService(asset_service=asset_service)
    await service.adjust_bundle_streaming(
        bundle_service=bundle_service,
        bundle_name="test_adj",
        batch_days=30,
        batch_assets=10,
    )
    bundle_service.register_bundle.assert_awaited_once()
    bundle_service.store_bundle.assert_awaited_once()

