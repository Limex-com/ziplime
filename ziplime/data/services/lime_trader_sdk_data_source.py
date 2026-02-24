import asyncio
import datetime
import logging

import structlog
from exchange_calendars import ExchangeCalendar
from lime_trader import AsyncLimeClient, LimeClient
from lime_trader.models.market import Period

import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.data.services.data_bundle_source import DataBundleSource


class LimeTraderSdkDataSource(DataBundleSource):
    def __init__(self, lime_sdk_credentials_file: str | None,
                 trading_calendar: ExchangeCalendar
                 ):
        super().__init__()
        self._lime_sdk_credentials_file = lime_sdk_credentials_file
        self.trading_calendar = trading_calendar
        self._logger = structlog.get_logger(__name__)
        if lime_sdk_credentials_file is None:
            self._lime_sdk_client = AsyncLimeClient.from_env(logger=self._logger)
            self._lime_sdk_client_sync = LimeClient.from_env(logger=self._logger)
        else:
            self._lime_sdk_client = AsyncLimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)
            self._lime_sdk_client_sync = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt, data_frequency,
                       exchange_name: str,
                       exchange_country: str,
                       ) -> pl.DataFrame:

        symbols = [asset.get_symbol_by_exchange(exchange_name=exchange_name) for asset in assets]
        quotes = self._lime_sdk_client_sync.market.get_current_quotes(symbols=symbols)

        cols = {"open": [], "close": [], "price": [], "high": [], "low": [], "volume": [], "date": [], "exchange": [],
                "symbol": [], "exchange_country": []}

        for result in quotes:
            cols["open"].append(result.open)
            cols["close"].append(result.close)
            cols["price"].append(result.close)
            cols["high"].append(result.high)
            cols["low"].append(result.low)
            cols["volume"].append(result.volume)
            cols["date"].append(result.date.astimezone(self.trading_calendar.tz))
            cols["exchange"].append(exchange_name)
            cols["exchange_country"].append(exchange_country)
            cols["symbol"].append(result.symbol)
        df = pl.DataFrame(cols, schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                        ("price", pl.Float64()),
                                        ("high", pl.Float64()), ("low", pl.Float64()),
                                        ("volume", pl.Float64()),
                                        ("date", pl.Datetime), ("exchange", pl.String),
                                        ("exchange_country", pl.String), ("symbol", pl.String)
                                        ])
        return df.select(pl.col(col) for col in fields)

    def get_data_sync(self, symbols: list[str],
                      frequency: datetime.timedelta,
                      date_from: datetime.datetime,
                      date_to: datetime.datetime,
                      ) -> pl.DataFrame:

        results = [self._lime_sdk_client_sync.market.get_quotes_history(
            symbol=symbol, period=self._frequency_to_period(frequency=frequency), from_date=date_from,
            to_date=date_to
        ) for symbol in symbols]

        cols = {"open": [], "close": [], "price": [], "high": [], "low": [], "volume": [], "date": [], "exchange": [],
                "symbol": [], "exchange_country": []}
        for results, symbol in zip(results, symbols):
            for result in results:
                cols["open"].append(result.open)
                cols["close"].append(result.close)
                cols["price"].append(result.close)
                cols["high"].append(result.high)
                cols["low"].append(result.low)
                cols["volume"].append(result.volume)
                cols["date"].append(result.timestamp.astimezone(date_to.tzinfo))
                cols["exchange"].append("LIME")
                cols["exchange_country"].append("US")
                cols["symbol"].append(symbol)
        df = pl.DataFrame(cols, schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                        ("price", pl.Float64()),
                                        ("high", pl.Float64()), ("low", pl.Float64()),
                                        ("volume", pl.Float64()),
                                        ("date", pl.Datetime), ("exchange", pl.String),
                                        ("exchange_country", pl.String), ("symbol", pl.String)
                                        ])
        return df.filter(pl.col("date") >= date_from, pl.col("date") <= date_to)

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       ) -> pl.DataFrame:

        quotes_tasks = [self._lime_sdk_client.market.get_quotes_history(
            symbol=symbol, period=self._frequency_to_period(frequency=frequency), from_date=date_from,
            to_date=date_to
        ) for symbol in symbols]

        results = await asyncio.gather(*quotes_tasks)

        for result in results:
            if len(df) > 0:
                df = df.rename(
                    {
                        "o": "open",
                        "h": "high",
                        "l": "low",
                        "c": "close",
                        "v": "volume",
                        "Date": "date"
                    }
                )
                df = df.with_columns(
                    pl.lit(symbol).alias("symbol"),
                    pl.lit("LIME").alias("exchange"),
                    pl.lit("US").alias("exchange_country"),
                    date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)),
                ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
                return df

    def get_data_by_limit(self, fields: frozenset[str],
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          exchange_name: str,
                          exchange_country: str,
                          trading_calendar: ExchangeCalendar
                          ) -> pl.DataFrame:

        symbols = [asset.get_symbol_by_exchange(exchange_name=exchange_name) for asset in assets if
                   asset.get_symbol_by_exchange(exchange_name=exchange_name)]
        # period = self._lime_trader_sdk_data_source._frequency_to_period(frequency=frequency)
        # lowest frequency of lime trader sdk is 1 minute
        try:
            sdk_period = self._frequency_to_period(frequency=frequency)
        except Exception as e:
            # TODO: handle case when we have for example 2d frequency, use 1 day with multiplier of 2
            # if datetime.timedelta(minutes=1) < frequency:
            #     multiplier = int(frequency / self.frequency)
            #     total_bar_count = limit * multiplier
            raise
        if frequency >= datetime.timedelta(days=1):
            multiplier = frequency // datetime.timedelta(days=1)
            time_window = trading_calendar.sessions_window(session=end_date.date(), count=-limit * multiplier)
            start_dt = time_window[0].replace(tzinfo=end_date.tzinfo)
            end_dt = time_window[-1].replace(tzinfo=end_date.tzinfo)

        else:
            multiplier = frequency // datetime.timedelta(minutes=1)
            time_window = trading_calendar.minutes_window(minute=end_date, count=-limit * multiplier)
            start_dt = time_window[0].astimezone(end_date.tzinfo)
            end_dt = time_window[-1].astimezone(end_date.tzinfo)

        sdk_results = [self._lime_sdk_client_sync.market.get_quotes_history(
            symbol=symbol, period=self._frequency_to_period(frequency=frequency), from_date=time_window[0],
            to_date=time_window[-1]
        ) for symbol in symbols]

        cols = {"open": [], "close": [], "price": [], "high": [], "low": [], "volume": [], "date": [], "exchange": [],
                "symbol": [], "exchange_country": []}
        for results, symbol in zip(sdk_results, symbols):
            for result in results:
                cols["open"].append(result.open)
                cols["close"].append(result.close)
                cols["price"].append(result.close)
                cols["high"].append(result.high)
                cols["low"].append(result.low)
                cols["volume"].append(result.volume)
                cols["date"].append(result.timestamp.astimezone(end_date.tzinfo))
                cols["exchange"].append(exchange_name)
                cols["exchange_country"].append(exchange_country)
                cols["symbol"].append(symbol)
        df_raw = pl.DataFrame(cols, schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                            ("price", pl.Float64()),
                                            ("high", pl.Float64()), ("low", pl.Float64()),
                                            ("volume", pl.Float64()),
                                            ("date", pl.Datetime), ("exchange", pl.String),
                                            ("exchange_country", pl.String), ("symbol", pl.String)
                                            ])
        df_raw = df_raw.filter(pl.col("date") >= start_dt, pl.col("date") <= end_dt)
        if multiplier != 1:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields).tail(
                limit)
            return df.select(pl.col(col) for col in fields)

        return df_raw.select(pl.col(col) for col in fields)

    def _frequency_to_period(self, frequency: datetime.timedelta) -> Period:
        match frequency:
            case datetime.timedelta(seconds=60):
                return Period.MINUTE
            case datetime.timedelta(seconds=300):
                return Period.MINUTE_5
            case datetime.timedelta(seconds=900):
                return Period.MINUTE_15
            case datetime.timedelta(seconds=1800):
                return Period.MINUTE_30
            case datetime.timedelta(seconds=3600):
                return Period.HOUR
            case datetime.timedelta(days=1):
                return Period.DAY
            case datetime.timedelta(days=7):
                return Period.WEEK
            case datetime.timedelta(days=30):
                return Period.MONTH
            case datetime.timedelta(days=90):
                return Period.QUARTER
            case datetime.timedelta(days=365):
                return Period.YEAR
            case _:
                raise ValueError(f"Unknown frequency {frequency} for lime trader sdk")
