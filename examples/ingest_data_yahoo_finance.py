"""Ingest daily bars for a few US equities through the Yahoo Finance connector."""
import asyncio
import datetime
import logging

from examples.providers_config import ASSET_DB_PATH

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.utils.bundle_utils import get_market_data_source
from ziplime.utils.logging_utils import configure_logging

SYMBOLS_STOCK = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]
SYMBOLS_CURRENCIES = ["EURUSD=X"]

BUNDLE_NAME = "yahoo_finance_daily_data"


async def ingest_data_yahoo_finance():
    start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(year=2025, month=8, day=30, tzinfo=datetime.timezone.utc)

    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    # Yahoo does not return the exchange alongside the bars, so hand the connector the listings we
    # already have; otherwise it falls back to one Ticker.info lookup per symbol.
    stock_listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s, mic=None) for s in SYMBOLS_STOCK], asset_type=AssetType.EQUITY)
    currency_listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=s, mic=None) for s in SYMBOLS_CURRENCIES], asset_type=AssetType.CURRENCY)
    known = [listing
             for listing in stock_listings + currency_listings
             if listing is not None]

    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=SYMBOLS_STOCK + SYMBOLS_CURRENCIES,
        trading_calendar="NYSE",
        bundle_name=BUNDLE_NAME,
        data_bundle_source=get_market_data_source("yahoo", assets=known),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        assets=known
    )


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_data_yahoo_finance())
