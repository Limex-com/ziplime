import asyncio
import logging
import pathlib

from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime.data.data_sources.yahoo_finance_asset_data_source import YahooFinanceAssetDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_yahoo_finance():
    asset_data_source = YahooFinanceAssetDataSource.from_env()

    asset_service = get_asset_service(
        clear_asset_db=True,
    )
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(ingest_assets_data_yahoo_finance())
