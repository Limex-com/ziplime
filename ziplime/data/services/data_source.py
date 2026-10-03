import datetime
from zoneinfo import ZoneInfo

import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.utils.date_utils import normalize_datetime, period_to_timedelta
from ziplime.assets.entities.exchange_asset import ExchangeAsset


class DataSource:

    def __init__(self, name: str, start_date: datetime.datetime, end_date: datetime.datetime,
                 frequency: datetime.timedelta | Period,
                 original_frequency: datetime.timedelta | Period,
                 data_type: DataType,
                 trading_calendar: ExchangeCalendar,
                 aggregation_specification: dict[str, str] = None):
        """
        Attributes:
            name (str): The name of the data source.
            start_date (datetime.datetime): Start instant for the data.
            end_date (datetime.datetime): End instant for the data.
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
        self.trading_calendar = trading_calendar
        timezone = trading_calendar.tz
        self.start_date = normalize_datetime(
            start_date, timezone
        )
        self.end_date = normalize_datetime(
            end_date, timezone
        )
        self.frequency = frequency
        self.frequency_td = period_to_timedelta(self.frequency)
        self.data_type = data_type
        self.aggregation_specification = aggregation_specification
        self.original_frequency = original_frequency

    def _normalize_query_datetime(self, value: datetime.datetime) -> datetime.datetime:
        """Interpret query bounds in the calendar timezone and match the data column timezone."""
        normalized = normalize_datetime(value, self.trading_calendar.tz)
        date_dtype = self.get_dataframe().schema.get("date")
        data_timezone = getattr(date_dtype, "time_zone", None)
        if data_timezone is not None:
            normalized = normalized.astimezone(ZoneInfo(data_timezone))
        return normalized

    def get_dataframe(self) -> pl.DataFrame:
        return self.data

    def last_available_bar(self, sid: int | None = None) -> datetime.datetime | None:
        """When this source's bars stop -- for one instrument, or across all of them.

        This is the *data's* own edge, not the window the source was declared with. The two differ
        exactly when something stops trading: a delisted equity keeps a row in the asset database
        and an ``end_date`` in 2099, and the only place its delisting is recorded is that the bars
        stop. Everything that has to notice a delisting asks here.

        Args:
            sid: The instrument to answer for, or ``None`` for the latest bar of any instrument.

        Returns:
            The instant of the last bar, or ``None`` when this source cannot say -- a live feed, a
            source that is not bar-shaped, or an instrument it does not carry. ``None`` means "no
            opinion" and must never be read as "it stopped trading".
        """
        return None

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[ExchangeAsset],
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
            assets (frozenset[ExchangeAsset]): Set of assets to retrieve data for.
            include_bounds (bool): If True, includes boundary dates in the data; else
                excludes them.

        Returns:
            pl.DataFrame: A dataframe containing filtered and aggregated data sorted
            by date.
        """
        from_date = self._normalize_query_datetime(from_date)
        to_date = self._normalize_query_datetime(to_date)
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

    async def get_data_by_limit(self, fields: frozenset[str] | None,
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta | Period,
                                assets: frozenset[ExchangeAsset],
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
            assets (frozenset[ExchangeAsset]): A set of assets for which to retrieve the data.
            include_end_date (bool): Whether the data for the end_date is included in results.

        Returns:
            pl.DataFrame: The resulting data frame containing the requested data fields and filtered rows.
        """
        end_date = self._normalize_query_datetime(end_date)
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

    async def get_data_by_window(self, fields: frozenset[str] | None,
                                 since: datetime.timedelta,
                                 end_date: datetime.datetime,
                                 frequency: datetime.timedelta | Period,
                                 assets: frozenset[ExchangeAsset],
                                 include_end_date: bool,
                                 ) -> pl.DataFrame:
        """Fetch everything in the last ``since`` of calendar time, however many rows that is.

        The counterpart to :meth:`get_data_by_limit`, and the right one for data that does not
        arrive on a schedule. A bar source produces one row per session, so "the last 30 rows" and
        "the last 30 sessions" mean the same thing. An event source does not: 30 rows of
        congressional disclosures for one ticker can span four years, and 30 rows of insider
        filings for a quiet issuer can span two. Asking for a count there is asking a question
        about the filers rather than about time, and a strategy that means "the last quarter" has
        to over-fetch and re-filter by date -- which every strategy in ``examples/huggingface``
        used to do by hand.

        The returned shape is identical to :meth:`get_data_by_limit`: flat rows carrying ``date``,
        ``sid`` and the requested fields, sorted by date. The two are interchangeable at the call
        site, which is the point.

        Args:
            fields: Columns to return besides ``date`` and ``sid``. ``None`` returns all of them.
            since: How far back from ``end_date`` to read.
            end_date: The near edge of the window, normally the simulation's current time.
            frequency: Downsampling frequency, applied only when the source is finer than it.
            assets: Instruments to return rows for.
            include_end_date: Whether a row stamped exactly at ``end_date`` is visible. False is
                what keeps a strategy from reading the bar it is currently trading into.

        Returns:
            The rows in ``(end_date - since, end_date)``, sorted by date.
        """
        end_date = self._normalize_query_datetime(end_date)
        if since <= datetime.timedelta(0):
            raise ValueError(f"since must be a positive duration, got {since!r}.")

        frequency_td = period_to_timedelta(frequency)
        start_date = end_date - since
        df = self.get_dataframe()
        if fields is None:
            fields = frozenset(df.columns)
        cols = list(fields.union({"date", "sid"}))
        sids = [asset.sid for asset in assets]

        near_edge = (pl.col("date") <= end_date) if include_end_date else (pl.col("date") < end_date)
        rows = df.select(pl.col(col) for col in cols).filter(
            near_edge, pl.col("date") > start_date, pl.col("sid").is_in(sids)
        ).sort(by="date")

        if self.frequency_td < frequency_td:
            rows = rows.group_by_dynamic(
                index_column="date", every=frequency, by="sid"
            ).agg(pl.col(field).last() for field in fields)
        return rows

    async def get_spot_value(self, assets: frozenset[ExchangeAsset], fields: frozenset[str], dt: datetime.datetime,
                             frequency: datetime.timedelta):
        """
        Retrieves the most recent spot value for specified assets and fields.

        Fetches data by the provided datetime, fields, and assets. It returns
        the most recent value for each asset-field combination up to, and
        including, the given datetime.

        Args:
            assets (frozenset[ExchangeAsset]): A collection of Asset objects representing
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

    def get_data_by_date_and_sids(self, fields: frozenset[str],
                                  start_date: datetime.datetime,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta | Period,
                                  sids: frozenset[int],
                                  include_bounds: bool,
                                  ) -> pl.DataFrame:
        ...
