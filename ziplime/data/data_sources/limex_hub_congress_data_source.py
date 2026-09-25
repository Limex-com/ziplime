import datetime
import multiprocessing
import os
import sys
from typing import Self

import limexhub
import requests
import structlog
from asyncclick import progressbar
from joblib import Parallel, delayed

import polars as pl
from ziplime.data.services.data_bundle_source import DataBundleSource

congress_data_fields = [
    "id",
    "chamber",
    "bioguide_id",
    "member_name",
    "member_first_name",
    "member_last_name",
    "party",
    "state",
    "state_district",
    "owner",
    "ticker",
    "asset_name",
    "asset_category",
    "transaction_type",
    "transaction_date",
    "notification_date",
    "filing_year",
    "min_amount_usd",
    "max_amount_usd",
    "is_option",
    "option_type",
    "option_quantity",
    "strike_price",
    "expiration_date",
    "days_to_expiration",
    "description",
    "doc_id",
    "report_url",
    "inserted_at",
]


def fetch_congress_data_task(
    date_from: datetime.datetime,
    date_to: datetime.datetime,
    limex_api_key: str,
    symbol: str,
) -> pl.DataFrame:
    limex_client = limexhub.RestAPI(token=limex_api_key)
    df = pl.from_pandas(
        limex_client.congress_trading(ticker=symbol, limit=10000),
        include_index=False,
        schema_overrides={'option_quantity': pl.Float64()}
    )
    if len(df) == 0:
        return df

    df = df.with_columns(
        pl.lit(symbol).alias("symbol"),
        date=pl.col("notification_date").str.to_datetime(format="%Y-%m-%d", strict=False).dt.replace_time_zone(str(date_from.tzinfo)),
        amount=((pl.col("min_amount_usd") + pl.col("max_amount_usd")) / 2),
    ).filter(
        pl.col("date") >= date_from,
        pl.col("date") <= date_to,
    )

    return df.sort(["symbol", "date"])


class LimexHubCongressDataSource(DataBundleSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None):
        self._limex_api_key = limex_api_key
        self._logger = structlog.get_logger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_data(
        self,
        symbols: list[str],
        frequency: datetime.timedelta,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        **kwargs,
    ) -> pl.DataFrame:

        def fetch_congress_data(limex_api_key: str, symbol: str) -> pl.DataFrame | None:
            try:
                return fetch_congress_data_task(
                    date_from=date_from,
                    date_to=date_to,
                    limex_api_key=limex_api_key,
                    symbol=symbol,
                )
            except Exception:
                self._logger.exception(
                    f"Exception fetching congress trading data for symbol {symbol}, "
                    f"date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

        total_days = (date_to - date_from).days
        chunks: list[pl.DataFrame] = []

        with progressbar(
            length=len(symbols) * total_days,
            label="Downloading congress trading data from LimexHub",
            file=sys.stdout,
        ) as pbar:
            res = Parallel(
                n_jobs=self._maximum_threads,
                prefer="threads",
                return_as="generator_unordered",
            )(delayed(fetch_congress_data)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                pbar.update(total_days)
                if item is None or len(item) == 0:
                    continue
                if chunks:
                    item = item.select(chunks[0].columns)
                chunks.append(item)

        if not chunks:
            return pl.DataFrame()

        return pl.concat(chunks)

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
