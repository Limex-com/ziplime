import datetime
import time
from typing import Any

import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar, get_calendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_storage import BundleStorage
from ziplime.utils.class_utils import load_class
from ziplime.utils.date_utils import period_to_timedelta


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
            data = await self._backfill_sid_data(data=data, asset_service=asset_service,
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

        await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage)
        await bundle_storage.store_bundle(data_bundle=data_bundle)

        self._logger.info(f"Finished ingesting custom bundle_name={name}, bundle_version={bundle_version}")

        return data_bundle

    async def _backfill_sid_data(self, data: pl.DataFrame, asset_service: AssetService, required_sessions: pl.Series):
        """Backfills missing symbol ID (sid) data in a DataFrame by performing lookups and handling missing
        data sessions. Used when symbols are provided in the input DataFrame but not sids.

        The method updates the input DataFrame by:
        1. Mapping symbols to their corresponding sid using the asset service.
        2. Backfilling missing data for required sessions.
        3. Logging warnings for missing data sessions.
        4. Raising an exception for any symbols absent from the asset database.

        Args:
            data (pl.DataFrame): A Polars DataFrame containing at least 'symbol' and 'date' columns.
            asset_service (AssetService): An instance of AssetService used to fetch equities by symbol.
            required_sessions (pl.Series): A Polars Series containing the required session dates.

        Returns:
            pl.DataFrame: Updated DataFrame with backfilled sid data.

        Raises:
            ValueError: If any symbols are missing in the asset database.
        """
        unique_symbols = list(data["symbol"].unique())
        symbol_to_sid = {a.get_symbol_by_exchange(exchange_name=None): a.sid for a in
                         await asset_service.get_equities_by_symbols(unique_symbols)}
        data = data.with_columns(
            pl.lit(0).alias("sid"),
        )

        for symbol in unique_symbols:
            symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
            missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))

            if len(missing_sessions) > 0:
                self._logger.warning(
                    f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
                new_rows_df = pl.DataFrame(
                    {"date": missing_sessions, "symbol": symbol},
                    schema_overrides={"date": data.schema["date"]}
                )
                # Concatenate with the original DataFrame
                data = pl.concat([data, new_rows_df], how="diagonal")
            missing_symbols = set(unique_symbols) - set(symbol_to_sid)
            if missing_symbols:
                raise ValueError(f"Symbols are missing in asset database: {missing_symbols}")

            data = data.with_columns(
                pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64).alias("sid")
            ).sort(["sid", "date"])
        return data

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
            "date", "symbol", "exchange", "exchange_country", "open", "high", "low", "close", "volume"
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

        equities_by_exchange = data.select(
            "symbol", "exchange", "exchange_country"
        ).group_by("exchange", "exchange_country").agg(pl.col("symbol").unique())
        for row in equities_by_exchange.iter_rows(named=True):
            exchange_name = row["exchange"]
            exchange_country = row["exchange_country"]
            symbols = row["symbol"]
            equities = await asset_service.get_equities_by_symbols_and_exchange(symbols=symbols,
                                                                                exchange_name=exchange_name)
            symbol_to_sid = {e.get_symbol_by_exchange(exchange_name=exchange_name): e.sid for e in equities}

            for symbol in symbols:
                symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
                missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))
                if len(missing_sessions) > 0:
                    self._logger.warning(
                        f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
                    new_rows_df = pl.DataFrame({"date": missing_sessions, "symbol": symbol, "exchange": exchange_name,
                                                "exchange_country": exchange_country},
                                               schema_overrides={"date": data.schema["date"]})

                    # Concatenate with the original DataFrame
                    data = pl.concat([data, new_rows_df], how="diagonal")
            missing_symbols = set(symbols) - set(symbol_to_sid)
            if missing_symbols:
                raise ValueError(f"Symbols are missing in asset database: {missing_symbols}")

            data = data.with_columns(
                pl.when(
                    pl.col("exchange") == exchange_name
                ).then(
                    pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64, strict=False)
                ).otherwise(
                    pl.col("sid")
                )
                .alias("sid")
            ).sort(["exchange","sid", "date"])
        if forward_fill_missing_ohlcv_data:
            data = data.with_columns(pl.col("close", "price").fill_null(strategy="forward"))
            data = data.with_columns(pl.col("high", "low", "open").fill_null(pl.col("price")))
            data = data.with_columns(pl.col("volume").fill_null(pl.lit(0.0)))

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
        await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage)
        await bundle_storage.store_bundle(data_bundle=data_bundle)
        duration = time.time() - start_duration
        self._logger.info(f"Finished ingesting market data bundle_name={name}, bundle_version={bundle_version}."
                          f"Total duration: {duration:.2f} seconds", duration=duration)

        return data_bundle

    async def load_bundle(self, bundle_name: str, bundle_version: str | None,
                          symbols: list[str] | None = None,
                          start_date: datetime.datetime | None = None,
                          end_date: datetime.datetime | None = None,
                          frequency: datetime.timedelta | Period | None = None,
                          start_auction_delta: datetime.timedelta = None,
                          end_auction_delta: datetime.timedelta = None,
                          aggregations: list[pl.Expr] = None
                          ) -> DataBundle:
        """
        Asynchronously loads a data bundle based on specified parameters and validates the configuration
        including time ranges, frequencies, and auction deltas. Retrieves necessary metadata, dependencies,
        and initializes a `DataBundle` instance with associated data and metadata.

        Args:
            bundle_name (str): Name of the data bundle to load.
            bundle_version (str | None): Version of the bundle to load. Optional if not version-specific.
            symbols (list[str] | None):
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

        bundle_start_date = datetime.datetime.strptime(bundle_metadata["start_date"], "%Y-%m-%dT%H:%M:%SZ")
        trading_calendar = get_calendar(bundle_metadata["trading_calendar_name"],
                                        start=bundle_start_date - datetime.timedelta(days=30))
        bundle_start_date = bundle_start_date.replace(tzinfo=trading_calendar.tz)
        bundle_end_date = datetime.datetime.strptime(bundle_metadata["end_date"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=trading_calendar.tz)
        frequency_timedelta = datetime.timedelta(seconds=int(bundle_metadata["frequency_seconds"])) if bundle_metadata[
                                                                                                           "frequency_seconds"] is not None else None
        frequency_text = bundle_metadata.get("frequency_text", None)
        timestamp = datetime.datetime.strptime(bundle_metadata["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=trading_calendar.tz)
        data_type = DataType(bundle_metadata["data_type"])
        bundle_frequency = frequency_timedelta or frequency_text

        if start_date is not None and start_date < bundle_start_date:
            raise ValueError(f"Start date {start_date} is before bundle start date {bundle_start_date}")
        if end_date is not None and end_date > bundle_end_date:
            raise ValueError(f"End date {end_date} is after bundle end date {bundle_end_date}")
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
                                 start_date=start_date or bundle_start_date,
                                 end_date=end_date or bundle_end_date,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency or bundle_frequency,
                                 original_frequency=bundle_frequency,
                                 timestamp=timestamp,
                                 version=bundle_metadata["version"],
                                 data_type=data_type
                                 )
        bundle_data_load_start = time.time()

        data = await bundle_storage.load_data_bundle(data_bundle=data_bundle,
                                                     symbols=symbols,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     frequency=frequency,
                                                     start_auction_delta=start_auction_delta,
                                                     end_auction_delta=end_auction_delta,
                                                     aggregations=aggregations
                                                     )
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
        return data_bundle

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
