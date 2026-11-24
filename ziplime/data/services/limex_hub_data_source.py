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
from ziplime.constants.period import Period
from ziplime.data.services.limex_history_tasks import (
    fetch_historical_limex_data_task,
)


class LimexHubDataSource(DataBundleSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None) -> None:
        self._limex_api_key = limex_api_key
        self._logger = structlog.get_logger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta | Period,
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
            except Exception:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {symbol}, "
                    f"date_from={date_from}, date_to={date_to}. Skipping."
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
