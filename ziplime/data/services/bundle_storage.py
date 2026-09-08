import datetime

import polars as pl
from abc import abstractmethod
from typing import Any, AsyncIterator, Self

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle


class BundleStorage:

    @abstractmethod
    async def store_bundle(self, data_bundle: DataBundle, merge: bool, merge_columns: list[str] | None = None):
        """
        Method for storing a data bundle asynchronously.

        Args:
            data_bundle (DataBundle): The data bundle to be stored.
            merge: If True data will be merged with existing bundle data
            merge_columns:  List of columns which are identifier which we should use to merge data
        """
        ...

    @abstractmethod
    async def load_data_bundle(self, data_bundle: DataBundle,
                               assets: list[ExchangeAsset | None] = None,
                               start_date: datetime.datetime | None = None,
                               end_date: datetime.datetime | None = None,
                               frequency: datetime.timedelta | Period | None = None,
                               start_auction_delta: datetime.timedelta = None,
                               end_auction_delta: datetime.timedelta = None,
                               aggregations: list[pl.Expr] = None,
                               ) -> pl.DataFrame:
        """
        Loads data from a DataBundle based on the specified parameters and returns
        a Polars DataFrame.

        Args:
            data_bundle (DataBundle): The source bundle containing the data to be loaded.
            assets (list[ExchangeAsset] | None, optional): A list of symbols to filter and load data for. If not
                provided, data for all available symbols is loaded.
            start_date (datetime.datetime | None, optional): The starting date-time for the data to be loaded.
                If not specified, it loads data from the earliest available date-time.
            end_date (datetime.datetime | None, optional): The ending date-time for the data to be loaded. If
                not specified, it loads data up to the latest available date-time.
            frequency (datetime.timedelta | Period | None, optional): The data sampling frequency for the
                loaded data. Defaults to the finest granularity if not specified.
            start_auction_delta (datetime.timedelta):
               Used when requested frequency is greater than ingested frequency.
               It allows defining custom start time for each frequency group.
               Example:
                 Ingested frequenct is 1m, requested frequency is 1d and start_auction_delta is 1h.
                 Before grouping data by 1d frequency, data will be filtered to include only data in each group that
                 is greater than 1h after the start of the group.
                 Can be useful if you want to test strategy on 1d frequenct but when running algorithm callback each day
                 1 hour after opening time
            end_auction_delta (datetime.timedelta):
                Used when requested frequency is greater than ingested frequency.
                It allows defining custom start time for each frequency group.
                Example:
                  Ingested frequenct is 1m, requested frequency is 1d and end_auction_delta is 1h.
                  Before grouping data by 1d frequency, data will be filtered to include only data in each group that
                  is lower than 1h before the end of the group.
                  Useful if you want to test strategy on 1d frequenct but when running algorithm callback each day
                  1 hour before closing time
            aggregations (list[pl.Expr], optional): A list of Polars aggregation expressions to be applied
                during the data loading process. Defaults to `None`.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the loaded and potentially filtered/aggregated data.

        Raises:
            NotImplementedError: Must be raised if this method is called directly from an abstract class.
        """
        ...

    @abstractmethod
    async def iter_data_bundle_batches(
            self,
            data_bundle: DataBundle,
            batch_days: int | None = 30,
            batch_assets: int | None = 100,
            sids: list[int] | None = None,
    ) -> AsyncIterator[pl.DataFrame]:
        """Yield bounded data batches without loading the complete bundle."""
        ...

    @abstractmethod
    async def get_data_bundle_sids(self, data_bundle: DataBundle) -> list[int]:
        """Return the distinct sids contained in a data bundle."""
        ...

    @abstractmethod
    async def load_data_bundle_before(
            self,
            data_bundle: DataBundle,
            sid: int,
            date: datetime.datetime | datetime.date,
            columns: list[str],
    ) -> pl.DataFrame:
        """Load the latest row for a sid strictly before a date."""
        ...

    @abstractmethod
    async def initialize_adjustment_columns(
            self,
            data_bundle: DataBundle,
            adjusted_columns: dict[str, str],
            adjusted_flag_column: str = "adjusted",
            sids: list[int] | None = None,
    ) -> None:
        """Add adjustment columns and initialize them from the original values."""
        ...

    @classmethod
    @abstractmethod
    async def from_json(cls, data: dict[str, Any]) -> Self:
        """
        Asynchronously create an instance of the class from JSON data.

        Args:
            data (dict[str, Any]): A dictionary containing JSON data
                used to construct the instance.

        Returns:
            Self: An instance of the class constructed from the provided
                JSON data.
        """
        ...

    @abstractmethod
    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]:
        """
        Converts a DataBundle instance to its JSON representation asynchronously.

        Args:
            data_bundle (DataBundle): The data bundle to be converted to JSON.

        Returns:
            dict[str, Any]: The JSON representation of the data bundle.
        """
        ...
