import datetime
import time
from typing import Any, AsyncIterator

import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from typing import Sequence

from ziplime.assets.domain.asset_type import AssetType


def _asset_type_names(asset_type: "AssetType | Sequence[AssetType]") -> str:
    """Render one or several asset types for an error message."""
    if isinstance(asset_type, AssetType):
        return asset_type.value
    return " / ".join(candidate.value for candidate in asset_type)
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_storage import BundleStorage
from ziplime.utils.class_utils import load_class
from ziplime.utils.date_utils import period_to_timedelta
from ziplime.utils.data_utils import backfill_sid_data
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.utils.calendar_utils import get_calendar


class BundleService:
    """
    Service class responsible for handling operations related to bundles.

    This class is designed to manage the lifecycle of data bundles, including
    listing existing bundles, ingesting custom data bundles, and ingesting market
    data bundles. It provides functionality to validate input data, process, and
    store bundles, as well as perform necessary backfilling for incomplete data.
    """

    def __init__(self, bundle_registry: BundleRegistry):
        """
        Args:
            bundle_registry (BundleRegistry): Registry for managing bundles.
        """
        self._bundle_registry = bundle_registry
        self._logger = structlog.get_logger(__name__)

    async def list_bundles(self) -> list[dict[str, Any]]:

        """Retrieves a list of bundles available in the bundle registry.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing bundle metadata.
                Each dictionary represents a registered bundle with its associated
                metadata.
        """
        return await self._bundle_registry.list_bundles()

    async def load_bundle_definition(
            self,
            bundle_name: str,
            bundle_version: str | None = None,
    ) -> tuple[DataBundle, BundleStorage]:
        metadata = await self._bundle_registry.load_bundle_metadata(
            bundle_name=bundle_name, bundle_version=bundle_version
        )
        if metadata is None:
            raise ValueError(f"Bundle {bundle_name} not found.")

        storage_class: type[BundleStorage] = load_class(
            module_name=".".join(metadata["bundle_storage_class"].split(".")[:-1]),
            class_name=metadata["bundle_storage_class"].split(".")[-1],
        )
        storage = await storage_class.from_json(metadata["bundle_storage_data"])
        calendar = get_calendar(
            metadata["trading_calendar_name"],
            start=metadata["start_date"],
            end=metadata["start_date"],
        )
        frequency = (
            datetime.timedelta(seconds=int(metadata["frequency_seconds"]))
            if metadata["frequency_seconds"] is not None
            else metadata["frequency_text"]
        )
        bundle = DataBundle(
            name=bundle_name,
            version=metadata["version"],
            start_date=metadata["start_date"].replace(tzinfo=calendar.tz),
            end_date=metadata["end_date"].replace(tzinfo=calendar.tz),
            trading_calendar=calendar,
            frequency=frequency,
            original_frequency=frequency,
            data_type=DataType(metadata["data_type"]),
            timestamp=metadata["timestamp"].replace(tzinfo=calendar.tz),
        )
        if bundle.data_type != DataType.MARKET_DATA:
            raise ValueError(f"Bundle {bundle_name} is not MARKET_DATA.")
        return bundle, storage

    async def iter_bundle_batches(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
            batch_days: int | None = 30,
            batch_assets: int | None = 100,
            sids: list[int] | None = None,
    ) -> AsyncIterator[pl.DataFrame]:
        async for batch in bundle_storage.iter_data_bundle_batches(
                data_bundle=data_bundle,
                batch_days=batch_days,
                batch_assets=batch_assets,
                sids=sids,
        ):
            yield batch

    async def get_bundle_sids(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
    ) -> list[int]:
        return await bundle_storage.get_data_bundle_sids(data_bundle)

    async def load_bundle_before(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
            sid: int,
            date: datetime.datetime | datetime.date,
            columns: list[str],
    ) -> pl.DataFrame:
        return await bundle_storage.load_data_bundle_before(
            data_bundle=data_bundle,
            sid=sid,
            date=date,
            columns=columns,
        )

    async def initialize_adjustment_columns(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
            adjusted_columns: dict[str, str],
            adjusted_flag_column: str = "adjusted",
            sids: list[int] | None = None,
    ) -> None:
        await bundle_storage.initialize_adjustment_columns(
            data_bundle=data_bundle,
            adjusted_columns=adjusted_columns,
            adjusted_flag_column=adjusted_flag_column,
            sids=sids,
        )

    async def register_bundle(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
            merge: bool = False,
    ) -> None:
        await self._bundle_registry.register_bundle(
            data_bundle=data_bundle,
            bundle_storage=bundle_storage,
            merge=merge,
        )

    async def store_bundle(
            self,
            data_bundle: DataBundle,
            bundle_storage: BundleStorage,
            merge: bool,
            merge_columns: list[str] | None = None,
    ) -> None:
        await bundle_storage.store_bundle(
            data_bundle=data_bundle,
            merge=merge,
            merge_columns=merge_columns,
        )

    async def load_bundle_metadata(
            self,
            bundle_name: str,
            bundle_version: str | None = None,
    ) -> dict[str, Any] | None:
        return await self._bundle_registry.load_bundle_metadata(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
        )

    async def ingest_custom_data_bundle(self, name: str,
                                        bundle_version: str,
                                        date_start: datetime.datetime,
                                        date_end: datetime.datetime,
                                        trading_calendar: ExchangeCalendar,
                                        symbols: list[str],
                                        data_bundle_source: DataBundleSource,
                                        frequency: datetime.timedelta | Period,
                                        data_frequency_use_window_end: bool,
                                        bundle_storage: BundleStorage,
                                        asset_service: AssetService,
                                        merge: bool,
                                        merge_columns: list[str]
                                        ):
        """Ingests a custom data bundle into the specified storage. This function processes and validates the provided data,
        ensures it aligns with the given trading calendar and frequency, and stores it using the provided storage system.

        Args:
            name: str
                The name of the custom bundle to ingest.
            bundle_version: str
                The version identifier for the custom bundle.
            date_start: datetime.datetime
                The start date of the data to include in the bundle. Must be within the bounds of the trading calendar.
            date_end: datetime.datetime
                The end date of the data to include in the bundle. Must be within the bounds of the trading calendar.
            trading_calendar: ExchangeCalendar
                The trading calendar defining the valid trading sessions and holidays.
            symbols: list of str
                The list of symbols to include in the data bundle.
            data_bundle_source: DataBundleSource
                The source from where the data is fetched for the ingestion.
            frequency: datetime.timedelta or Period
                The frequency of the data to be ingested (e.g., '1w', '1d').
            data_frequency_use_window_end: bool
                Indicates whether the frequency uses the window end for calculations.
                If False, it will use the window start.

                Example: Frequency is 1M (1 month) and data_frequency_use_window_end is False.
                         Each row will have beginning of the month in 'date' column

                         Frequency is 1M (1 month) and data_frequency_use_window_end is False.
                         Each row will have end of the month in 'date' column

                This is only valid if frequency is greater than 1 day because that is the largest unit of date where
                we can get session using exchange calendar.
            bundle_storage: BundleStorage
                The storage system where the ingested bundle will be saved.
            asset_service: AssetService
                The service that provides asset metadata

        Raises:
            ValueError:
                - Raised when date_start is before the first session of the trading calendar or when date_end is past the last session.
                - Also raised when required data columns are missing or when neither a symbol nor sid column is provided.

        Returns:
            DataBundle:
                Prepared and stored data bundle instance.

        """
        self._logger.info(f"Ingesting custom bundle: name={name}, date_start={date_start}, date_end={date_end}, "
                          f"symbols={symbols}, frequency={frequency}")
        if date_start < trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date start must be after first session of trading calendar. "
                f"First session is {trading_calendar.first_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date start is {date_start}")

        if date_end > trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date end must be before last session of trading calendar. "
                f"Last session is {trading_calendar.last_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date end is {date_end}")

        data = await data_bundle_source.get_data(
            symbols=symbols,
            frequency=frequency,
            date_from=date_start,
            date_to=date_end
        )

        if data.is_empty():
            self._logger.warning(
                f"No data for symbols={symbols}, frequency={frequency}, date_start={date_start},"
                f"date_end={date_end} found. Skipping ingestion."
            )
            return

        # repair data
        all_bars = [
            s for s in pl.from_pandas(
                trading_calendar.sessions_minutes(start=date_start.replace(tzinfo=None),
                                                  end=date_end.replace(tzinfo=None)).tz_convert(trading_calendar.tz)
            ) if s >= date_start and s <= date_end
        ]

        required_sessions = pl.DataFrame({"date": all_bars}).group_by_dynamic(
            index_column="date", every=frequency
        ).agg()
        if data_frequency_use_window_end:
            if (
                    (type(frequency) is datetime.timedelta and frequency >= datetime.timedelta(days=1)) or
                    (type(frequency) is str and frequency in ["1d", "1w", "1mo", "1q", "1y"])
            ):
                last_row = required_sessions.tail(1).with_columns(
                    pl.col("date").dt.offset_by(frequency) - pl.duration(days=1))
                required_sessions = required_sessions.with_columns(
                    pl.col("date") - pl.duration(days=1)
                )[1:]

                required_sessions = pl.concat([required_sessions, last_row])
        required_columns = [
            "date"
        ]
        missing = [c for c in required_columns if c not in data.columns]

        if missing:
            raise ValueError(f"Ingested data is missing required columns: {missing}. Cannot ingest bundle.")
        if "symbol" not in data.columns and "sid" not in data.columns:
            raise ValueError(f"When ingesting custom bundle you must supply either a symbol or a sid column.")

        sid_id = "sid" in data.columns
        symbol_id = "symbol" in data.columns

        asset_identifiers = list(data["sid"].unique()) if sid_id else list(data["symbol"].unique())

        if sid_id:
            data = await self._backfill_symbol_data(data=data, asset_service=asset_service,
                                                    required_sessions=required_sessions)
        else:
            data = await backfill_sid_data(data=data, asset_service=asset_service,
                                           required_sessions=required_sessions)

        data_bundle = DataBundle(name=name,
                                 start_date=date_start,
                                 end_date=date_end,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency,
                                 original_frequency=frequency,
                                 data=data,
                                 timestamp=datetime.datetime.now(tz=trading_calendar.tz),
                                 version=bundle_version,
                                 data_type=DataType.CUSTOM
                                 )
        existing_bundle_metadata = await self._bundle_registry.load_bundle_metadata(bundle_name=name,
                                                                                    bundle_version=bundle_version)
        if not existing_bundle_metadata:
            await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage,
                                                        merge=merge)
        await bundle_storage.store_bundle(data_bundle=data_bundle, merge=merge,
                                          merge_columns=merge_columns)
        if existing_bundle_metadata:
            # We need to update start/end date
            new_start_date = min(date_start, existing_bundle_metadata["start_date"].replace(tzinfo=trading_calendar.tz))
            new_end_date = max(date_end, existing_bundle_metadata["end_date"].replace(tzinfo=trading_calendar.tz))
            data_bundle.start_date = new_start_date
            data_bundle.end_date = new_end_date
            await self._bundle_registry.register_bundle(
                data_bundle=data_bundle, bundle_storage=bundle_storage,
                merge=merge
            )
        self._logger.info(f"Finished ingesting custom bundle_name={name}, bundle_version={bundle_version}")

        return data_bundle

    async def _backfill_symbol_data(self):
        pass

    async def ingest_market_data_bundle(self, name: str,
                                        bundle_version: str,
                                        date_start: datetime.datetime,
                                        date_end: datetime.datetime,
                                        trading_calendar: ExchangeCalendar,
                                        symbols: list[str],
                                        data_bundle_source: DataBundleSource,
                                        frequency: datetime.timedelta,
                                        bundle_storage: BundleStorage,
                                        asset_service: AssetService,
                                        forward_fill_missing_ohlcv_data: bool,
                                        merge: bool,
                                        asset_type: AssetType | Sequence[AssetType] = AssetType.EQUITY,
                                        assets: list[ExchangeAsset] | None = None,
                                        ):

        """
        Asynchronously ingests a market data bundle based on provided parameters and performs validation, repair,
        and transformation of data before storing it and registering the bundle.

        This function fetches the required data from a source, ensures complete and accurate data integrity based
        on the provided trading calendar, forward fills missing OHLCV (Open, High, Low, Close, Volume) data if
        specified, and generates a properly formatted `DataBundle` to be stored and registered.

        Args:
            name (str): The name of the market data bundle to ingest.
            bundle_version (str): The version identifier for the market data bundle.
            date_start (datetime.datetime): The start date for the data to be ingested.
            date_end (datetime.datetime): The end date for the data to be ingested.
            trading_calendar (ExchangeCalendar): The trading calendar to be used for session validation and processing.
            symbols (list[str]): The list of symbols for the equities to be included in the data bundle.
            data_bundle_source (DataBundleSource): The source from which market data will be retrieved.
            frequency (datetime.timedelta): The frequency of the market data bars (e.g., 1m, 1d etc.).
            bundle_storage (BundleStorage): The storage component to persist the ingested and processed market data bundle.
            asset_service (AssetService): The service to retrieve asset metadata such as equities by symbols and exchange mapping.
            forward_fill_missing_ohlcv_data (bool): If True, fills missing OHLCV data forward.
            assets (list[ExchangeAsset] | None): Listings the symbols refer to, already resolved.
                Preferred over ``asset_type``, and required in practice for a bundle spanning
                asset classes: a ticker is unique only within a class, and 168 of them in a
                database holding both equities and futures exist under two. Passing the listings
                says exactly which instrument each symbol is instead of asking the database to
                guess.
            asset_type (AssetType | Sequence[AssetType]): Which kind of asset the symbols name,
                used only when ``assets`` is not given. Several may be passed, but a symbol that
                resolves under more than one raises rather than being guessed at. Symbol lookup is
                type-scoped because the same ticker on the same exchange can belong to more than
                one asset -- the same ticker can be both an equity row and a futures
                contract, and resolving futures bars as equities silently files them under the
                wrong sid.

        Raises:
            ValueError:
                - If the start date is before the first session of the trading calendar or the end date is after
                  the last session.
                - If the retrieved market data is missing required columns.
                - If there are symbols in the market data that are not present in the asset database.

        Returns:
            DataBundle: Prepared and stored data bundle instance.
        """
        self._logger.info(f"Ingesting market data bundle: name={name}, date_start={date_start}, date_end={date_end}, "
                          f"symbols={symbols}, frequency={frequency}")
        start_duration = time.time()
        if date_start < trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date start must be after first session of trading calendar. "
                f"First session is {trading_calendar.first_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date start is {date_start}")

        if date_end > trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date end must be before last session of trading calendar. "
                f"Last session is {trading_calendar.last_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date end is {date_end}")

        data = await data_bundle_source.get_data(
            symbols=symbols,
            frequency=frequency,
            date_from=date_start,
            date_to=date_end
        )

        if data.is_empty():
            self._logger.warning(
                f"No data for symbols={symbols}, frequency={frequency}, date_from={date_start}, date_end={date_end} found. Skipping ingestion.")
            return

        required_columns = [
            "date", "symbol", "mic", "open", "high", "low", "close", "volume"
        ]
        missing = [c for c in required_columns if c not in data.columns]

        if missing:
            raise ValueError(f"Ingested data is missing required columns: {missing}. Cannot ingest bundle.")

        data = data.with_columns(
            pl.lit(0).alias("sid"),
            pl.lit(False).alias("backfilled")
        )
        # repair data
        all_bars = [
            s for s in pl.from_pandas(
                trading_calendar.sessions_minutes(start=date_start.replace(tzinfo=None),
                                                  end=date_end.replace(tzinfo=None)).tz_convert(trading_calendar.tz)
            ) if s >= date_start and s <= date_end
        ]
        required_sessions = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
            index_column="date", every=frequency
        ).agg()

        assets_by_exchange = data.select(
            "symbol", "mic"
        ).group_by("mic").agg(pl.col("symbol").unique())
        for row in assets_by_exchange.iter_rows(named=True):
            exchange_mic = row["mic"]
            symbols = row["symbol"]
            if assets is not None:
                # The caller already knows which instrument each symbol is. Nothing to resolve,
                # and nothing to get wrong.
                symbol_to_sid = {a.symbol: a.sid for a in assets if a.mic == exchange_mic}
            else:
                exchange_assets = await asset_service.get_exchange_assets_by_symbols(
                    symbols=[AssetSymbol(mic=exchange_mic, symbol=symbol) for symbol in symbols],
                    asset_type=asset_type)
                symbol_to_sid = {a.symbol: a.sid for a in exchange_assets if a is not None}

            for symbol in symbols:
                symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
                missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))
                if len(missing_sessions) > 0:
                    self._logger.warning(
                        f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
                    new_rows_df = pl.DataFrame({"date": missing_sessions, "symbol": symbol, "mic": exchange_mic},
                                               schema_overrides={"date": data.schema["date"]})

                    # Concatenate with the original DataFrame
                    data = pl.concat([data, new_rows_df], how="diagonal")
            missing_symbols = set(symbols) - set(symbol_to_sid)
            if missing_symbols:
                raise ValueError(
                    f"Symbols are missing in asset database as "
                    f"{_asset_type_names(asset_type)}: {missing_symbols}@{exchange_mic}")

            data = data.with_columns(
                pl.when(
                    pl.col("mic") == exchange_mic
                ).then(
                    pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64, strict=False)
                ).otherwise(
                    pl.col("sid")
                )
                .alias("sid")
            ).sort(["mic", "sid", "date"])
        if forward_fill_missing_ohlcv_data:
            data = data.with_columns(pl.col("close", "price").fill_null(strategy="forward"))
            data = data.with_columns(pl.col("high", "low", "open").fill_null(pl.col("price")))
            data = data.with_columns(pl.col("volume").fill_null(pl.lit(0.0)))
            # check
            data = data.with_columns(pl.col("low", "open", "close", "high", "price").fill_null(pl.lit(0.0)))
            data = data.with_columns(pl.col("backfilled").fill_null(pl.lit(False)))

        data_bundle = DataBundle(name=name,
                                 start_date=date_start,
                                 end_date=date_end,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency,
                                 original_frequency=frequency,
                                 data=data,
                                 timestamp=datetime.datetime.now(tz=trading_calendar.tz),
                                 version=bundle_version,
                                 data_type=DataType.MARKET_DATA
                                 )

        existing_bundle_metadata = await self._bundle_registry.load_bundle_metadata(bundle_name=name,
                                                                                    bundle_version=bundle_version)
        if not existing_bundle_metadata:
            await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage,
                                                        merge=merge)
        await bundle_storage.store_bundle(data_bundle=data_bundle,
                                          merge=merge, merge_columns=["sid", "date"])
        if existing_bundle_metadata:
            # We need to update start/end date
            new_start_date = min(date_start, existing_bundle_metadata["start_date"].replace(tzinfo=trading_calendar.tz))
            new_end_date = max(date_end, existing_bundle_metadata["end_date"].replace(tzinfo=trading_calendar.tz))
            data_bundle.start_date = new_start_date
            data_bundle.end_date = new_end_date
            await self._bundle_registry.register_bundle(
                data_bundle=data_bundle, bundle_storage=bundle_storage,
                merge=merge
            )

        duration = time.time() - start_duration
        self._logger.info(f"Finished ingesting market data bundle_name={name}, bundle_version={bundle_version}."
                          f"Total duration: {duration:.2f} seconds", duration=duration)

        return data_bundle

    async def load_bundle(self, bundle_name: str, bundle_version: str | None,
                          assets: list[ExchangeAsset] | None = None,
                          start_date: datetime.datetime | None = None,
                          end_date: datetime.datetime | None = None,
                          frequency: datetime.timedelta | Period | None = None,
                          start_auction_delta: datetime.timedelta = None,
                          end_auction_delta: datetime.timedelta = None,
                          aggregations: list[pl.Expr] = None,
                          asset_service: AssetService | None = None,
                          roll_finder_settings: dict[str, dict] | None = None
                          ) -> tuple[DataBundle, dict[ExchangeAsset, tuple[datetime.datetime, datetime.datetime]]]:
        """
        Asynchronously loads a data bundle based on specified parameters and validates the configuration
        including time ranges, frequencies, and auction deltas. Retrieves necessary metadata, dependencies,
        and initializes a `DataBundle` instance with associated data and metadata.

        Args:
            bundle_name (str): Name of the data bundle to load.
            bundle_version (str | None): Version of the bundle to load. Optional if not version-specific.
            assets (list[ExchangeAsset] | None):
              Filter data bundle to include only specific symbols. Defaults to None (includes all symbols).
            start_date (datetime.datetime | None):
              Filter data bundle to include only data starting with specific date. Defaults to None.
            end_date (datetime.datetime | None):
              Filter data bundle to include only data till specific date. Defaults to None.
            frequency (datetime.timedelta | Period | None): Desired frequency for data. Defaults to None (frequency in which bundle was ingested will be used).
            start_auction_delta (datetime.timedelta):
               Used when requested frequency is greater than ingested frequency.
               It allows defining custom start time for each frequency group.
               Example:
                 Ingested frequenct is 1m, requested frequency is 1d, and start_auction_delta is 1h.
                 Before grouping data by 1d frequency, data will be filtered to include only data in each group that
                 is greater than 1h after the start of the group.
                 Can be useful if you want to test strategy on 1d frequenct but when running algorithm callback each day
                 1 hour after opening time
            end_auction_delta (datetime.timedelta):
                Used when requested frequency is greater than ingested frequency.
                It allows defining custom start time for each frequency group.
                Example:
                  Ingested frequenct is 1m, requested frequency is 1d, and end_auction_delta is 1h.
                  Before grouping data by 1d frequency, data will be filtered to include only data in each group that
                  is lower than 1h before the end of the group.
                  Useful if you want to test strategy on 1d frequenct but when running algorithm callback each day
                  1 hour before closing time
            aggregations (list[pl.Expr]):
                List of aggregations to apply on the data. If not specified default aggregations will be used.
            asset_service (AssetService | None):
                Required only to trade continuous futures: the bundle needs it to resolve a
                continuous future to the contract it held on a given date.
            roll_finder_settings (dict[str, dict] | None):
                Per-roll-style overrides, e.g. ``{"calendar": {"roll_offset_days": 10}}`` to roll
                ten days before auto close, or ``{"volume": {"grace_period_days": 3}}``.

        Returns:
            DataBundle: An initialized `DataBundle` instance containing data and metadata for the specified
            bundle.

        Raises:
            ValueError: If the bundle, version, frequency, or date range is invalid.
        """
        self._logger.info(f"Loading bundle: bundle_name={bundle_name}, bundle_version={bundle_version}")

        bundle_metadata_start = time.time()

        bundle_metadata = await self._bundle_registry.load_bundle_metadata(bundle_name=bundle_name,
                                                                           bundle_version=bundle_version)
        if bundle_metadata is None:
            if bundle_version is None:
                raise ValueError(f"Bundle {bundle_name} not found.")
            else:
                raise ValueError(f"Bundle {bundle_name} with version {bundle_version} not found.")
        self._logger.info(f"Loaded bundle metadata in {time.time() - bundle_metadata_start} seconds")
        bundle_storage_class: BundleStorage = load_class(
            module_name='.'.join(bundle_metadata["bundle_storage_class"].split(".")[:-1]),
            class_name=bundle_metadata["bundle_storage_class"].split(".")[-1])

        bundle_storage = await bundle_storage_class.from_json(bundle_metadata["bundle_storage_data"])
        tc =  get_calendar(bundle_metadata["trading_calendar_name"],
                           start=bundle_metadata["start_date"],
                           end=bundle_metadata["start_date"])
        if start_date is None:
            start_date = bundle_metadata["start_date"].replace(tzinfo=tc.tz)
        if end_date is None:
            end_date = bundle_metadata["end_date"].replace(tzinfo=tc.tz)

        trading_calendar = get_calendar(bundle_metadata["trading_calendar_name"],
                                        start=start_date.date() - datetime.timedelta(days=30))

        frequency_timedelta = datetime.timedelta(seconds=int(bundle_metadata["frequency_seconds"])) if bundle_metadata[
                                                                                                           "frequency_seconds"] is not None else None
        frequency_text = bundle_metadata.get("frequency_text", None)
        timestamp = bundle_metadata["timestamp"].replace(tzinfo=trading_calendar.tz)
        data_type = DataType(bundle_metadata["data_type"])
        bundle_frequency = frequency_timedelta or frequency_text

        if frequency is not None and period_to_timedelta(frequency) < period_to_timedelta(bundle_frequency):
            raise ValueError(f"Requested frequency {frequency} is less than bundle frequency {bundle_frequency}")

        if start_auction_delta is not None and period_to_timedelta(start_auction_delta) < period_to_timedelta(
                bundle_frequency):
            raise ValueError(
                f"Requested start auction delta frequency {frequency} is less than bundle frequency {bundle_frequency}")

        if end_auction_delta is not None and period_to_timedelta(end_auction_delta) < period_to_timedelta(
                bundle_frequency):
            raise ValueError(
                f"Requested end auction delta frequency {frequency} is less than bundle frequency {bundle_frequency}")

        data_bundle = DataBundle(name=bundle_name,
                                 start_date=start_date,
                                 end_date=end_date,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency or bundle_frequency,
                                 original_frequency=bundle_frequency,
                                 timestamp=timestamp,
                                 version=bundle_metadata["version"],
                                 data_type=data_type,
                                 asset_service=asset_service,
                                 roll_finder_settings=roll_finder_settings
                                 )
        bundle_data_load_start = time.time()

        data = await bundle_storage.load_data_bundle(data_bundle=data_bundle,
                                                     assets=assets,
                                                     start_date=start_date,
                                                     end_date=end_date + datetime.timedelta(days=1),
                                                     frequency=frequency or bundle_frequency,
                                                     start_auction_delta=start_auction_delta,
                                                     end_auction_delta=end_auction_delta,
                                                     aggregations=aggregations
                                                     )

        missing_data = await self.check_for_missing_data(data=data,
                                                         assets=assets,
                                                         frequency=frequency or bundle_frequency,
                                                         trading_calendar=trading_calendar,
                                                         start_date=start_date, end_date=end_date)

        load_duration = time.time() - bundle_data_load_start

        sid_indexes = data.with_row_index().group_by("sid", maintain_order=True).agg([
            pl.col("index").first().alias("start_index"),
            pl.col("index").last().alias("end_index")
        ])

        self._logger.info(f"Loaded data bundle in {load_duration:.2f} seconds",
                          duration=load_duration)
        data_bundle.data = data
        data_bundle.sid_indexes = {row["sid"]: (row["start_index"], row["end_index"] + 1) for row in
                                   sid_indexes.iter_rows(named=True)}

        # data_bundle.asset_sid_date_index = {
        #     (row["sid"], row["date"]): row["index"]
        #     for row in data.with_row_index().iter_rows(named=True)
        # }

        return data_bundle, missing_data

    async def check_for_missing_data(self,
                                     data: pl.DataFrame,
                                     trading_calendar: ExchangeCalendar,
                                     frequency: datetime.timedelta | Period,
                                     start_date: datetime.datetime,
                                     end_date: datetime.datetime,
                                     assets: list[ExchangeAsset] | None,
                                     ) -> dict[ExchangeAsset, tuple[datetime.datetime, datetime.datetime]]:
        if assets is None:
            return {}
        all_bars = [
            s for s in pl.from_pandas(
                trading_calendar.sessions_minutes(start=start_date.replace(tzinfo=None).date(),
                                                  end=end_date.replace(tzinfo=None).date()).tz_convert(
                    trading_calendar.tz)
            ) if s >= start_date and s <= end_date
        ]
        required_sessions = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
            index_column="date", every=frequency
        ).agg()
        missing_data_per_symbol = {}

        for asset in assets:
            symbol_data = data.filter(sid=asset.sid).with_columns(pl.col("date"))
            missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))
            if len(missing_sessions) > 0:
                missing_data_per_symbol[asset] = (missing_sessions[0], missing_sessions[-1])
                self._logger.warning(
                    f"Data for symbol {asset.symbol}@{asset.mic} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
        missing_sids = set([asset.sid for asset in assets]) - set(data["sid"].unique())
        if missing_sids:
            missing_assets = [asset for asset in assets if asset.sid in missing_sids]
            missing_symbols = [f'{asset.symbol}@{asset.mic}' for asset in missing_assets]
            for asset in missing_assets:
                missing_data_per_symbol[asset] = (start_date, end_date)

        return missing_data_per_symbol

    async def clean(self, bundle_name: str, before: datetime.datetime = None, after: datetime.datetime = None,
                    keep_last: bool = None):
        """
        Cleans up bundles based on the specified criteria.

        This method iterates through the bundles in the registry and removes
        those that match the given parameters.

        Args:
            bundle_name (str): The name of the bundle to clean.
            before (datetime.datetime, optional): A datetime to filter bundles created before
                this date. Defaults to None.
            after (datetime.datetime, optional): A datetime to filter bundles created after
                this date. Defaults to None.
            keep_last (bool, optional): A flag to indicate whether to keep the most recent
                bundle. Defaults to None.
        """

        for bundle in await self._bundle_registry.list_bundles():
            self._delete_bundle(bundle)
