import datetime
import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.utils.date_utils import period_to_timedelta


class DataSource:

    def __init__(self, name: str, start_date: datetime.date, end_date: datetime.date,
                 frequency: datetime.timedelta | Period,
                 original_frequency: datetime.timedelta | Period,
                 data_type: DataType,
                 aggregation_specification: dict[str, str] = None):
        """
        Attributes:
            name (str): The name of the data source.
            start_date (datetime.date): Start date for the data.
            end_date (datetime.date): End date for the data.
            frequency (datetime.timedelta | Period): Desired data frequency, e.g., 1m, 1d,.
            original_frequency (datetime.timedelta | Period):
              The original frequency of data. Used when data in the source is different from the requested frequency.
              If original frequency is lower than requested frequency, data will be grouped by the requested frequency.
            data_type (DataType): The type of data associated with the source.
            aggregation_specification (dict[str, str] | None): A dictionary describing
                aggregation rules, with keys as data attributes and values as the
                corresponding aggregation methods.
        """
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.frequency_td = period_to_timedelta(self.frequency)
        self.data_type = data_type
        self.aggregation_specification = aggregation_specification
        self.original_frequency = original_frequency

    def get_dataframe(self) -> pl.DataFrame:
        return self.data

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:
        """
        Fetch data within a specified date range and frequency for specified assets.

        This method retrieves data with specific fields based on provided date range,
        frequency, and assets. Additionally, it provides an option to include or exclude
        boundary dates in the filtration process. The returned data is grouped by asset
        identifier, and further aggregated based on the provided frequency, if
        necessary. The resultant data is sorted by the date column.

        Arguments:
            fields (frozenset[str]): Set of fields to retrieve from the data.
            from_date (datetime.datetime): Start date for data filtration.
            to_date (datetime.datetime): End date for data filtration.
            frequency (datetime.timedelta | Period): Frequency for grouping the data.
            assets (frozenset[Asset]): Set of assets to retrieve data for.
            include_bounds (bool): If True, includes boundary dates in the data; else
                excludes them.

        Returns:
            pl.DataFrame: A dataframe containing filtered and aggregated data sorted
            by date.
        """
        cols = set(fields.union({"date", "sid"}))
        if include_bounds:
            df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= to_date,
                pl.col("date") >= from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).all()
        else:
            df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < to_date,
                pl.col("date") > from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).all()
        if self.frequency < frequency:
            df = df.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
        return df.sort(by="date")

    def get_data_by_limit(self, fields: frozenset[str] | None,
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta | Period,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        """
        Fetch data for a specified set of assets, fields, and parameters, limiting the number of entries.

        The method retrieves data from a data source based on the provided limit, end_date,
        frequency, assets, and fields. It handles cases where the frequency of the requested data
        differs from the source frequency, adjusting the required bar count accordingly. If the
        end_date exceeds the available range, it calls another retrieval method. The data is filtered and
        grouped based on the assets, frequency, and inclusion of the end_date.

        Args:
            fields (frozenset[str] | None): A set of data fields to include in the result. If None,
                                            all available fields are included.
            limit (int): The maximum number of rows to retrieve for each asset.
            end_date (datetime.datetime): The end date for the data range.
            frequency (datetime.timedelta | Period): The required frequency for the retrieved data.
            assets (frozenset[Asset]): A set of assets for which to retrieve the data.
            include_end_date (bool): Whether the data for the end_date is included in results.

        Returns:
            pl.DataFrame: The resulting data frame containing the requested data fields and filtered rows.
        """
        frequency_td = period_to_timedelta(frequency)
        total_bar_count = limit
        if end_date > self.end_date:
            return self.get_missing_data_by_limit(frequency=frequency, assets=assets, fields=fields,
                                                  limit=limit, include_end_date=include_end_date,
                                                  end_date=end_date
                                                  )  # pl.DataFrame() # we have missing data

        if self.frequency_td < frequency_td:
            multiplier = int(frequency_td / self.frequency_td)
            total_bar_count = limit * multiplier
        df = self.get_dataframe()
        if fields is None:
            fields = frozenset(df.columns)
        cols = list(fields.union({"date", "sid"}))

        if include_end_date:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).tail(total_bar_count).sort(by="date")
        else:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).tail(
                total_bar_count).sort(by="date")

        if self.frequency_td < frequency_td:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields).tail(
                limit)
            return df
        return df_raw

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime,
                       frequency: datetime.timedelta):
        """
        Retrieves the most recent spot value for specified assets and fields.

        Fetches data by the provided datetime, fields, and assets. It returns
        the most recent value for each asset-field combination up to, and
        including, the given datetime.

        Args:
            assets (frozenset[Asset]): A collection of Asset objects representing
                the assets for which the spot value is requested.
            fields (frozenset[str]): A set of field names for which data is
                retrieved.
            dt (datetime.datetime): The datetime up to, and including, which
                the most recent spot values are retrieved.
            frequency (datetime.timedelta): The frequency of the data time series.

        Returns:
            pandas.DataFrame: A DataFrame containing the most recent spot values
            for the fields and assets provided, up to the specified datetime.
        """
        df_raw = self.get_data_by_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=assets,
            include_end_date=True,
        )
        return df_raw
