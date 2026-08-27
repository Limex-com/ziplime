import asyncio
import logging
import pathlib

from providers_config import ASSET_DB_PATH

from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime.utils.bundle_utils import get_asset_data_source
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_yahoo_finance():
    asset_data_source = get_asset_data_source("yahoo")

    # clear_asset_db stays False so this can be run alongside other connectors.
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(ingest_assets_data_yahoo_finance())
