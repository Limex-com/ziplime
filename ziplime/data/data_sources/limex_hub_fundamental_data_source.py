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
from ziplime.data.data_sources.limex_fundamental_tasks import fetch_fundamental_data_task


class LimexHubFundamentalDataSource(DataBundleSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None) -> None:
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

        def fetch_fundamental_data(limex_api_key: str, symbol: str) -> pl.DataFrame | None:
            try:
                result = fetch_fundamental_data_task(date_from=date_from, date_to=date_to,
                                                     limex_api_key=limex_api_key,
                                                     symbol=symbol)
                return result
            except Exception:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {symbol}, "
                    f"date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

        total_days = (date_to - date_from).days
        final = pl.DataFrame()

        with progressbar(length=len(symbols) * total_days, label="Downloading fundamental data from LimexHub",
                         file=sys.stdout) as pbar:
            res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads",
                           return_as="generator_unordered")(
                delayed(fetch_fundamental_data)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                pbar.update(total_days)
                if item is None:
                    continue
                if item is None or len(item) == 0:
                    continue
                item = item.select(final.columns)
                final = pl.concat([final, item], how="diagonal_relaxed")

        return final

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
