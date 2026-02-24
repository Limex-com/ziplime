import datetime

import structlog
from exchange_calendars import ExchangeCalendar

import polars as pl

from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.constants.period import Period
from ziplime.data.services.data_source import DataSource
from ziplime.utils.data_utils import _process_data


class CSVDataSource(DataSource):
    def __init__(self,
                 name: str,
                 csv_file_name: str,
                 column_mapping: dict[str, str],
                 frequency: datetime.timedelta | Period,
                 date_column_name: str,
                 date_format: str,
                 data_frequency_use_window_end: bool,
                 symbols: list[str],
                 asset_service: AssetService,
                 trading_calendar: ExchangeCalendar,
                 data_type: DataType,
                 ):
        self._csv_file_name = csv_file_name
        self._column_mapping = column_mapping
        self._logger = structlog.get_logger(__name__)
        self._date_format = date_format
        self._date_column_name = date_column_name
        self._asset_service = asset_service
        self._trading_calendar = trading_calendar
        self._data_frequency_use_window_end = data_frequency_use_window_end
        self._symbols = symbols
        self.data = None
        self.start_date = None
        self.end_date = None
        self.data_type = data_type
        super().__init__(name=name, start_date=self.start_date, end_date=self.end_date, frequency=frequency,
                         data_type=data_type, original_frequency=frequency)

    async def load_data_in_memory(self) -> pl.DataFrame:
        df = pl.read_csv(self._csv_file_name)  # s, schema_overrides={self._date_column_name: pl.Datetime})

        # Parse "date" column with your format
        df = df.with_columns(
            pl.col(self._date_column_name).str.strptime(pl.Datetime("us"), format=self._date_format).alias(
                self._date_column_name)
        )

        df = df.rename(self._column_mapping)
        df = df.with_columns(
            pl.col("close").alias("price"),
            date=pl.col("date").dt.replace_time_zone(str(self._trading_calendar.tz))
        )

        df = await _process_data(data=df,
                           date_start=df["date"].min(),
                           date_end=df["date"].max(),
                           data_frequency_use_window_end=self._data_frequency_use_window_end,
                           frequency=self.frequency,
                           trading_calendar=self._trading_calendar,
                           asset_service=self._asset_service,
                           name=self.name,
                           symbols=self._symbols,

                           )
        self.data = df
        self.start_date = self.data["date"].min()
        self.end_date = self.data["date"].max()
