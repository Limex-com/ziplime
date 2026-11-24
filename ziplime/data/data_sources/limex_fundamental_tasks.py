"""Worker tasks and normalization for fundamental data."""

import datetime
import limexhub
import polars as pl


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
    "volatility"
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


def fetch_fundamental_data_task(date_from: datetime.datetime,
                                date_to: datetime.datetime,
                                limex_api_key: str,
                                symbol: str,
                                ) -> pl.DataFrame:
    limex_client = limexhub.RestAPI(token=limex_api_key)
    df = pl.from_pandas(limex_client.fundamental(
        symbol=symbol,
        from_date=(date_from - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        to_date=(date_to + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        fields=','.join(fundamental_data_fields),
    ), include_index=True)
    if len(df) == 0:
        return df

    df = df.with_columns(
        pl.lit(symbol).alias("symbol"),
        date=pl.col("date").cast(pl.Datetime).dt.replace_time_zone(str(date_from.tzinfo)),
    ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)

    if len(df) == 0:
        return df

    return _normalize_fundamental_result(
        df=df,
        date_from=date_from,
        date_to=date_to,
        symbol=symbol,
    )
