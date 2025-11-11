import datetime
import multiprocessing
import os
import sys
from typing import Self

import limexhub
import structlog
from asyncclick import progressbar
from joblib import Parallel, delayed

import polars as pl
from ziplime.data.services.data_bundle_source import DataBundleSource


def fetch_historical_limex_data_task(date_from: datetime.datetime,
                                     date_to: datetime.datetime,
                                     limex_api_key: str,
                                     symbol: str,
                                     frequency: datetime.timedelta
                                     ) -> pl.DataFrame:
    limex_client = limexhub.RestAPI(token=limex_api_key)
    timeframe = 3
    if frequency == datetime.timedelta(minutes=1):
        timeframe = "1m"
    elif frequency == datetime.timedelta(hours=1):
        timeframe = "1h"
    elif frequency == datetime.timedelta(days=1):
        timeframe = "1d"
    elif frequency == datetime.timedelta(weeks=1):
        timeframe = "1w"
    elif frequency == datetime.timedelta(days=30):
        timeframe = "1M"
    elif frequency == datetime.timedelta(days=90):
        timeframe = "1q"
    df = pl.from_pandas(limex_client.candles(symbols=symbol,
                                             start=(date_from - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                                             end=(date_to + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                                             interval=timeframe), include_index=True,
                        schema_overrides={"o": pl.Float64(), "h": pl.Float64(), "l": pl.Float64(), "c": pl.Float64(),
                                          "v": pl.Float64()}
                        )
    if len(df) > 0:
        df = df.rename(
            {
                "Date": "date"
            }
        )
        df = df.with_columns(
            pl.lit(symbol).alias("symbol"),
            pl.lit("LIME").alias("exchange"),
            pl.lit("US").alias("exchange_country"),
            pl.col("close").alias("price"),
            date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)),
        ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
        return df
    return df


class LimexHubDataSource(DataBundleSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None):
        super().__init__()
        self._limex_api_key = limex_api_key
        self._logger = structlog.get_logger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:

        def fetch_historical(limex_api_key: str, symbol: str) -> pl.DataFrame | None:
            try:
                result = fetch_historical_limex_data_task(date_from=date_from, date_to=date_to,
                                                          limex_api_key=limex_api_key,
                                                          symbol=symbol,
                                                          frequency=frequency)
                return result
            except Exception as e:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {symbol}, date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

        total_days = (date_to - date_from).days
        final = pl.DataFrame()

        with progressbar(length=len(symbols) * total_days, label="Downloading historical data from LimexHub",
                         file=sys.stdout) as pbar:
            res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads",
                           return_as="generator_unordered")(
                delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                pbar.update(total_days)
                if item is None:
                    continue
                final = pl.concat([final, item])

        return final

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
