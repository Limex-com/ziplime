"""Worker tasks for historical Limex data."""

import datetime
import limexhub
import polars as pl
from ziplime.constants.period import Period


def fetch_historical_limex_data_task(date_from: datetime.datetime,
                                     date_to: datetime.datetime,
                                     limex_api_key: str,
                                     symbol: str,
                                     frequency: datetime.timedelta | Period
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
            pl.lit("XNGS").alias("mic"),  # TODO: return mic from LimexHub
            pl.col("close").alias("price"),
            date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)),
        ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
        return df
    return df
