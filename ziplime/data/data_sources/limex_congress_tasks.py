"""Worker tasks for fetching congressional trading data."""

import datetime
import limexhub
import polars as pl


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
        date=(
            pl.col("notification_date").str.to_datetime(format="%Y-%m-%d", strict=False)
            .dt.replace_time_zone(str(date_from.tzinfo))
        ),
        amount=((pl.col("min_amount_usd") + pl.col("max_amount_usd")) / 2),
    ).filter(
        pl.col("date") >= date_from,
        pl.col("date") <= date_to,
    )

    return df.sort(["symbol", "date"])
