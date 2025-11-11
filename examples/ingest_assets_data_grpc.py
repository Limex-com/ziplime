import asyncio
import logging

from ziplime.core.ingest_data import get_asset_service, ingest_assets
from ziplime.data.data_sources.grpc.grpc_asset_data_source import GrpcAssetDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_grpc():
    asset_data_source = GrpcAssetDataSource.from_env()
    asset_service = get_asset_service(
        clear_asset_db=True,
    )
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_assets_data_grpc())
