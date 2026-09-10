import datetime
from typing import Sequence
import os
from pathlib import Path

import asyncio
from exchange_calendars import ExchangeCalendar
from scipy.constants import micro

from ziplime.assets.models.exchange_asset_model import ExchangeAssetModel
from ziplime.utils.calendar_utils import get_calendar

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.models.exchange_info_model import ExchangeInfoModel
from ziplime.assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository
from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period
from ziplime.constants.stock_symbols import ALL_US_STOCK_SYMBOLS
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.data.services.file_system_delta_lake_bundle_storage import FileSystemDeltaLakeBundleStorage
from ziplime.assets.entities.exchange_asset import ExchangeAsset


def get_asset_service(db_path: str = str(Path(Path.home(), ".ziplime", "assets.sqlite").absolute()),
                      clear_asset_db: bool = False) -> AssetService:
    """
    Creates and configures an AssetService instance.

    This function sets up an `AssetService` by initializing its required repositories
    and optionally clearing an existing asset database if specified. The repositories provide
    the backend capabilities required by the AssetService for managing assets and adjustments.

    Args:
        db_path (str, optional): The path to the asset database file. Defaults to a SQLite file
            located at `~/.ziplime/assets.sqlite`.
        clear_asset_db (bool, optional): If True and the database file exists, the file will
            be removed prior to initializing a new asset database. Defaults to False.

    Returns:
        AssetService: An initialized AssetService instance.
    """
    if clear_asset_db and os.path.exists(db_path):
        os.remove(db_path)
    db_url = f"sqlite+aiosqlite:///{db_path}"
    assets_repository = SqlAlchemyAssetRepository(db_url=db_url, future_chain_predicates=CHAIN_PREDICATES)
    adjustments_repository = SqlAlchemyAdjustmentRepository(db_url=db_url)
    asset_service = AssetService(asset_repository=assets_repository, adjustments_repository=adjustments_repository)
    return asset_service


async def ingest_assets(asset_service: AssetService, asset_data_source: AssetDataSource):
    """
    Ingests and saves assets into the given asset service using data from the specified
    asset data source.

    The function fetches asset data from the provided data source and maps it to different asset
    types (e.g., currencies, equities). It creates objects needed for these assets like Currency,
    Equity, and their respective symbol mappings, and then saves these entities via the asset
    service.

    Args:
        asset_service (AssetService): The service responsible for saving asset-related data.
        asset_data_source (AssetDataSource): The data source providing information about the assets.

    Raises:
        Exception: Propagates any exceptions raised during data fetching or saving processes where applicable.
    """
    asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
    asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

    exchanges = await asset_data_source.get_exchanges()
    if len(exchanges) > 0:
        await asset_service.save_exchanges(exchanges=exchanges)
    assets_import = await asset_data_source.get_assets(exchanges=exchanges)
    await asset_service.import_assets(assets_import=assets_import)
    # await asset_service.save_equities(equities=assets)


# async def ingest_default_assets(asset_service: AssetService, asset_data_source: AssetDataSource):
#     """
#     Ingests default asset data into the asset service.
#
#     This method populates the asset service with predefined assets including a default
#     currency ("USD") and all US stock symbols. Each asset is configured with default
#     start and end dates, symbol mappings, and other attributes to ensure compatibility
#     in the target system's data structure.
#
#     Args:
#         asset_service (AssetService): The service responsible for managing assets. Used to
#             store the ingested assets, such as exchanges, currencies, and equities.
#         asset_data_source (AssetDataSource): Source of the asset information. While not used
#             currently in the function implementation, it potentially allows fetching data
#             dynamically in future extensions.
#
#     Returns:
#         None
#     """
#     asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
#     asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)
#
#     usd_currency = Currency(
#         asset_name="USD",
#         symbol_mapping={
#             "LIME": CurrencySymbolMapping(
#                 symbol="USD",
#                 exchange_name="LIME",
#                 start_date=asset_start_date,
#                 end_date=asset_end_date
#             )
#         },
#         sid=None,
#         start_date=asset_start_date,
#         end_date=asset_end_date,
#         auto_close_date=asset_end_date,
#         first_traded=asset_start_date,
#         mic=None
#     )
#
#     equities = [
#         Equity(
#             asset_name=symbol,
#             symbol_mapping={
#                 "LIME": EquitySymbolMapping(
#                     symbol=symbol,
#                     exchange_name="LIME",
#                     start_date=asset_start_date,
#                     end_date=asset_end_date,
#                     company_symbol="",
#                     share_class_symbol=""
#                 )
#             },
#             sid=None,
#             start_date=asset_start_date,
#             end_date=asset_end_date,
#             auto_close_date=asset_end_date,
#             first_traded=asset_start_date,
#             mic=None
#         ) for symbol in ALL_US_STOCK_SYMBOLS
#
#     ]
#
#     await asset_service.save_exchanges(
#         exchanges=[ExchangeInfo(exchange="LIME", canonical_name="LIME", country_code="US")])
#     await asset_service.save_currencies([usd_currency])
#     await asset_service.save_equities(equities)

async def ingest_symbol_universe(asset_service: AssetService, asset_data_source: AssetDataSource,
                                 symbol_universe_name: str):
    """
    Ingests a symbol universe by retrieving the constituents of a specified index and persisting it.

    Fetches the list of constituent symbols for the given index from the provided data source,
    retrieves the corresponding Equity objects, constructs a SymbolsUniverse instance, and saves it.

    Args:
        asset_data_source: Data source for retrieving index constituents.
        symbol_universe_name: The name or identifier of the index to ingest.
    """
    universe = await asset_data_source.get_symbol_universe(asset_service=asset_service,
                                                           symbol_universe_name=symbol_universe_name)
    await asset_service.save_symbol_universe(symbol_universe=universe)


async def ingest_market_data(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: str,
        bundle_name: str,
        symbols: list[str],
        data_frequency: datetime.timedelta,
        data_bundle_source: DataBundleSource,
        asset_service: AssetService,
        merge: bool = False,
        forward_fill_missing_ohlcv_data: bool = True,
        bundle_storage_path: str = str(Path(Path.home(), ".ziplime", "data")),
        asset_type: AssetType | Sequence[AssetType] = AssetType.EQUITY,
        assets: list = None,
):
    """
    Ingests market data into a specified bundle for a given time period, using
    specified trading symbols, calendar, and other configurations. The function ensures the
    data is processed and stored in the provided bundle storage location, aligning it with
    the referenced trading calendar. This allows for accurate and efficient management of
    financial data for analysis or simulation.

    Args:
        start_date (datetime.datetime): The starting date from which market data will be ingested.
        end_date (datetime.datetime): The ending date until which market data will be ingested.
        trading_calendar (str): The trading calendar to align the data with.
        bundle_name (str): The name of the bundle to store the ingested market data.
        symbols (list[str]): A list of trading symbols to fetch market data for.
        data_frequency (datetime.timedelta): The frequency at which data intervals should be recorded.
        data_bundle_source (DataBundleSource): The source responsible for providing market data.
        asset_service (AssetService): Service for handling and obtaining asset-related information.
        forward_fill_missing_ohlcv_data (bool, optional): Indicates whether to forward-fill missing
            Open-High-Low-Close-Volume (OHLCV) data in the ingested bundle. Defaults to True.
        bundle_storage_path (str, optional): The path where the data bundle should be stored.
            Defaults to the directory ".ziplime/data" within the user's home path.
        asset_type (AssetType | Sequence[AssetType]): Which kind of asset the symbols name, used
            to resolve them to sids when ``assets`` is not given. Pass AssetType.FUTURES_CONTRACT
            when ingesting futures.
        assets (list[ExchangeAsset] | None): The listings themselves, already resolved. Preferred,
            and effectively required for a bundle spanning asset classes -- a ticker is unique only
            within a class, so resolving one by name alone can return the wrong instrument.

    Raises:
        Exception: May raise exceptions related to data retrieval, storage processes, or configuration
            errors during the ingestion operation.
    """
    calendar = get_calendar(trading_calendar)

    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_storage = FileSystemDeltaLakeBundleStorage(base_data_path=bundle_storage_path, compression_level=5)

    if merge:
        existing_bundle_metadata = await bundle_registry.load_bundle_metadata(bundle_name=bundle_name,
                                                                              bundle_version=None)
        if not existing_bundle_metadata:
            bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))
        else:
            bundle_version = existing_bundle_metadata["version"]
    else:
        bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))
    await bundle_service.ingest_market_data_bundle(
        date_start=start_date.replace(tzinfo=calendar.tz),
        date_end=end_date.replace(tzinfo=calendar.tz),
        bundle_storage=bundle_storage,
        data_bundle_source=data_bundle_source,
        frequency=data_frequency,
        symbols=symbols,
        name=bundle_name,
        bundle_version=bundle_version,
        trading_calendar=calendar,
        asset_service=asset_service,
        merge=merge,
        forward_fill_missing_ohlcv_data=forward_fill_missing_ohlcv_data,
        asset_type=asset_type,
        assets=assets,
    )


async def ingest_custom_data(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: str,
        bundle_name: str,
        symbols: list[str],
        data_frequency: datetime.timedelta | Period,
        data_frequency_use_window_end: bool,
        data_bundle_source: DataBundleSource,
        asset_service: AssetService,
        merge: bool = False,
        merge_columns: list[str] = ["sid", "date"],
        bundle_storage_path: str = str(Path(Path.home(), ".ziplime", "data")),
):
    """
    Ingests a custom data bundle into the specified storage.

    This function facilitates the ingestion of custom financial data into the storage
    for later analysis or use. It requires details such as the time range, trading
    calendar, bundle name, list of symbols, frequency of data, source of the data,
    and additional configuration options. The ingestion process utilizes services
    for bundle management and storage.

    Args:
        start_date (datetime.datetime): The start date for the data ingestion period.
        end_date (datetime.datetime): The end date for the data ingestion period.
        trading_calendar (str): The trading calendar to use for determining trading days.
        bundle_name (str): The name of the bundle being ingested.
        symbols (list[str]): A list of symbols to include in the data ingestion.
        data_frequency (datetime.timedelta | Period): The frequency of the data points
            to ingest, either as a timedelta or a Period.
        data_frequency_use_window_end (bool): Indicates whether the frequency is
            aligned to the window end when using frequency greater than daily frequency (trading session)
        data_bundle_source (DataBundleSource): The source of the data to ingest.
        asset_service (AssetService): The asset service providing information about
            assets.
        bundle_storage_path (str, optional): The path to store the ingested bundle.
            Defaults to the data directory under the user's home path.

    """
    calendar = get_calendar(trading_calendar)

    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_storage = FileSystemDeltaLakeBundleStorage(base_data_path=bundle_storage_path, compression_level=5)

    if merge:
        existing_bundle_metadata = await bundle_registry.load_bundle_metadata(bundle_name=bundle_name,
                                                                              bundle_version=None)
        if not existing_bundle_metadata:
            bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))
        else:
            bundle_version = existing_bundle_metadata["version"]
    else:
        bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))

    # bundle_version = str(int(datetime.datetime.now(tz=calendar.tz).timestamp()))

    await bundle_service.ingest_custom_data_bundle(
        date_start=start_date.replace(tzinfo=calendar.tz),
        date_end=end_date.replace(tzinfo=calendar.tz),
        bundle_storage=bundle_storage,
        data_bundle_source=data_bundle_source,
        frequency=data_frequency,
        data_frequency_use_window_end=data_frequency_use_window_end,
        symbols=symbols,
        name=bundle_name,
        bundle_version=bundle_version,
        trading_calendar=calendar,
        asset_service=asset_service,
        merge=merge,
        merge_columns=merge_columns
    )
