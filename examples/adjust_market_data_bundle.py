import asyncio
import pathlib
import logging

from ziplime.core.ingest_data import get_asset_service
from ziplime.data.services.adjustments_service import AdjustmentsService
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.utils.logging_utils import configure_logging


async def adjust_market_data_bundle():
    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    bundle_storage_path = str(
        pathlib.Path(pathlib.Path.home(), ".ziplime", "data")
    )

    adjustments_service = AdjustmentsService(asset_service=asset_service)
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)

    await adjustments_service.adjust_bundle(bundle_service=bundle_service,
                                            bundle_name="limex_us_minute_data",
                                            bundle_version=None,
                                            adjusted_bundle_name="limex_us_minute_data_adjusted",
                                            save=True,
                                            merge=False,
                                            )


if __name__ == "__main__":
    configure_logging(level=logging.ERROR, file_name="mylog.log")
    asyncio.run(adjust_market_data_bundle())
