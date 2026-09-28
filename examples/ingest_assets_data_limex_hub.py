import asyncio
import logging
import pathlib

from ziplime.core.ingest_data import get_asset_service, ingest_assets, ingest_symbol_universe
from ziplime.data.data_sources.limex_hub_asset_data_source import LimexHubAssetDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_limex_hub():
    asset_data_source = LimexHubAssetDataSource.from_env()
    asset_service = get_asset_service(
        clear_asset_db=True,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)
    await ingest_symbol_universe(
        asset_service=asset_service, asset_data_source=asset_data_source, symbol_universe_name="Q100US")
    await ingest_symbol_universe(
        asset_service=asset_service, asset_data_source=asset_data_source, symbol_universe_name="Q500US")
    await ingest_symbol_universe(
        asset_service=asset_service, asset_data_source=asset_data_source, symbol_universe_name="Q1500US")


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(ingest_assets_data_limex_hub())
