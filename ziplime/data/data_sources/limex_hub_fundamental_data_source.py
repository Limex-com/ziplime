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

fundamental_data_fields = [
    "balance_sheet",
    "beta",
    "cash_flow",
    "dividend_yield",
    "dividend_yield_actual",
    "earnings_variability",
    "growth",
    "income_statement",
    "leverage",
    "machine_1",
    "machine_10",
    "machine_2",
    "machine_3",
    "machine_4",
    "machine_5",
    "machine_6",
    "machine_7",
    "machine_8",
    "machine_9",
    "momentum",
    "profitability",
    "short_interest",
    "size",
    "trading_activity",
    "value",
    "volatility",
]


def _normalize_fundamental_result(
    df: pl.DataFrame,
    date_from: datetime.datetime,
    date_to: datetime.datetime,
    symbol: str,
) -> pl.DataFrame:
    if len(df) == 0:
        return df

    if "date" not in df.columns:
        raise ValueError("Fundamental response is missing required column 'date'.")

    df = df.with_columns(
        pl.lit(symbol).alias("symbol"),
        date=pl.col("date").cast(pl.Datetime, strict=False).dt.replace_time_zone(str(date_from.tzinfo)),
    ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)

    if len(df) == 0:
        return df

    if "field" in df.columns and "value" in df.columns:
        df = df.pivot(
            on="field",
            index=["date", "symbol"],
            values="value",
            aggregate_function="last",
        )
    else:
        keep_columns = ["date", "symbol"]
        if "period" in df.columns:
            keep_columns.append("period")
        keep_columns.extend([field for field in fundamental_data_fields if field in df.columns])
        df = df.select(keep_columns).unique(subset=["date", "symbol"], keep="last")

    if "period" in df.columns:
        df = df.with_columns(pl.col("period").cast(pl.Utf8, strict=False))

    for field in fundamental_data_fields:
        if field not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(field))
        else:
            df = df.with_columns(pl.col(field).cast(pl.Float64, strict=False))

    ordered_columns = ["date", "symbol"]
    if "period" in df.columns:
        ordered_columns.append("period")
    ordered_columns.extend(fundamental_data_fields)

    return df.select(ordered_columns).sort(["symbol", "date"])


def fetch_fundamental_data_task(
    date_from: datetime.datetime,
    date_to: datetime.datetime,
    limex_api_key: str,
    symbol: str,
) -> pl.DataFrame:
    limex_client = limexhub.RestAPI(token=limex_api_key)
    df = pl.from_pandas(
        limex_client.fundamental(
            symbol=symbol,
            from_date=(date_from - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            to_date=(date_to + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
            fields=",".join(fundamental_data_fields),
        ),
        include_index=True,
    )
    if len(df) == 0:
        return df

    return _normalize_fundamental_result(
        df=df,
        date_from=date_from,
        date_to=date_to,
        symbol=symbol,
    )


class LimexHubFundamentalDataSource(DataBundleSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None):
        super().__init__()
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

        def fetch_fundamental_data(limex_api_key: str, symbol: str) -> pl.DataFrame | None:
            try:
                return fetch_fundamental_data_task(
                    date_from=date_from,
                    date_to=date_to,
                    limex_api_key=limex_api_key,
                    symbol=symbol,
                )
            except Exception:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {symbol}, date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

        total_days = (date_to - date_from).days
        final = pl.DataFrame()

        with progressbar(
            length=len(symbols) * total_days,
            label="Downloading fundamental data from LimexHub",
            file=sys.stdout,
        ) as pbar:
            res = Parallel(
                n_jobs=multiprocessing.cpu_count() * 2,
                prefer="threads",
                return_as="generator_unordered",
            )(delayed(fetch_fundamental_data)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                pbar.update(total_days)
                if item is None or len(item) == 0:
                    continue
                final = pl.concat([final, item], how="diagonal_relaxed")

        return final

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
