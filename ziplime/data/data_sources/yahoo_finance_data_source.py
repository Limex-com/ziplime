import datetime
import multiprocessing
import yfinance as yf

import structlog

import polars as pl
from ziplime.data.services.data_bundle_source import DataBundleSource


class YahooFinanceDataSource(DataBundleSource):
    def __init__(self, maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    def _get_frequency(self, frequency: datetime.timedelta) -> str:
        if frequency == datetime.timedelta(minutes=1):
            return "1m"
        elif frequency == datetime.timedelta(minutes=2):
            return "2m"
        elif frequency == datetime.timedelta(minutes=5):
            return "5m"
        elif frequency == datetime.timedelta(minutes=15):
            return "15m"
        elif frequency == datetime.timedelta(minutes=30):
            return "30m"
        elif frequency == datetime.timedelta(minutes=60):
            return "60m"
        elif frequency == datetime.timedelta(minutes=90):
            return "90m"
        elif frequency == datetime.timedelta(days=1):
            return "1d"
        elif frequency == datetime.timedelta(days=5):
            return "5d"

        raise ValueError(f"Unsupported frequency for Yahoo Finance {frequency}")

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:

        yfinance_data_raw = yf.download(tickers=symbols, threads=1,
                                        interval=self._get_frequency(frequency=frequency),
                                        start=date_from, end=date_to, group_by="Ticker", progress=True,
                                        auto_adjust=True,
                                        multi_level_index=False)
        final = pl.DataFrame()
        for symbol in symbols:
            df_symbol = yfinance_data_raw[symbol]

            df = pl.from_pandas(df_symbol, include_index=True,
                                schema_overrides={"Open": pl.Float64(), "High": pl.Float64(),
                                                  "Low": pl.Float64(), "Close": pl.Float64(),
                                                  "Volume": pl.Float64()}
                                )
            if "Date" in df.columns:
                date_column = "Date"
            else:
                date_column = "Datetime"

            if len(df) > 0:
                df = df.rename(
                    {
                        "Open": "open",
                        "High": "high",
                        "Low": "low",
                        "Close": "close",
                        "Volume": "volume",
                        f"{date_column}": "date"
                    }
                )
                df = df.with_columns(
                    pl.lit(symbol).alias("symbol"),
                    pl.lit("LIME").alias("exchange"),
                    pl.lit("US").alias("exchange_country"),
                    pl.col("close").alias("price"),
                )
                if date_column == "Datetime":
                    df = df.with_columns(date=pl.col("date").dt.convert_time_zone(str(date_from.tzinfo)))
                else:
                    df = df.with_columns(date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)))
                df = df.filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
                final = pl.concat([final, df])
        return final
