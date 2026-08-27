"""Ingest MOEX FORTS futures contracts into the asset database.

Run this before ingest_data_finam_futures.py: bars are keyed by the sid this step assigns.
"""
import asyncio
import logging

from finam_futures_config import (
    ASSET_DB_PATH, INGEST_END, INGEST_START, ROOT_SYMBOLS, require_finam_secret,
)

from ziplime.core.ingest_data import get_asset_service
from ziplime.utils.bundle_utils import get_asset_data_source
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_finam():
    require_finam_secret()
    asset_data_source = get_asset_data_source("finam")

    # clear_asset_db stays False: the ingest is idempotent, so re-running only adds what is new.
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    exchanges = await asset_data_source.get_exchanges(root_symbols=ROOT_SYMBOLS)
    await asset_service.save_exchanges(exchanges=exchanges)

    assets_import = await asset_data_source.get_assets(
        exchanges=exchanges,
        root_symbols=ROOT_SYMBOLS,
        start_date=INGEST_START,
        end_date=INGEST_END,
    )
    await asset_service.import_assets(assets_import=assets_import)

    for contract in assets_import.futures:
        print(f"{contract.asset_name:10s} {contract.root_symbol:8s} "
              f"{contract.start_date} -> {contract.expiration_date} "
              f"multiplier={contract.multiplier} tick={contract.tick_size}")


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_assets_data_finam())
