import asyncio
import datetime
import multiprocessing
import os
import sys
import time
from typing import Self

import aiocache
import grpc
import structlog
from aiocache import Cache
from asyncclick import progressbar
from google.type.date_pb2 import Date

import polars as pl

from ziplime.data.services.data_bundle_source import DataBundleSource
# from ziplime.data.data_sources.grpc.grpc_stubs.grpc.ta import candles_pb2
# from ziplime.data.data_sources.grpc.grpc_stubs.grpc.ta import candles_pb2_grpc
# from ziplime.data.data_sources.grpc.grpc_stubs.proto.ta import ta_pb2
# from ziplime.data.data_sources.grpc.grpc_stubs.proto.common import securityidentifier_pb2
#
from google.protobuf.timestamp_pb2 import Timestamp
from ziplime.data.data_sources.grpc.grpc_stubs.grpc.tradeapi.v1.auth import auth_service_pb2_grpc, auth_service_pb2
from ziplime.data.data_sources.grpc.grpc_stubs.grpc.tradeapi.v1.marketdata import marketdata_service_pb2_grpc, \
    marketdata_service_pb2
# from ziplime.data.data_sources.grpc.grpc_stubs.grpc.tradeapi.v1 import interval_pb2
from google.type import interval_pb2


class GrpcDataSource(DataBundleSource):
    def __init__(self, authorization_token: str, server_url: str,
                 maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        self._server_url = server_url
        self._authorization_token = authorization_token
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_token(self) -> str:
        credentials = grpc.ssl_channel_credentials()
        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = auth_service_pb2_grpc.AuthServiceStub(channel)
            auth_request = auth_service_pb2.AuthRequest()
            auth_request.secret = self._authorization_token
            response = await stub.Auth(auth_request)
        return response.token

    def _get_timeframe(self, frequency: datetime.timedelta) -> str:
        if frequency <= datetime.timedelta(minutes=1):
            return "TIME_FRAME_M1"
        elif frequency <= datetime.timedelta(minutes=5):
            return "TIME_FRAME_M5"
        elif frequency <= datetime.timedelta(minutes=15):
            return "TIME_FRAME_M15"
        elif frequency <= datetime.timedelta(minutes=30):
            return "TIME_FRAME_M30"
        elif frequency <= datetime.timedelta(hours=1):
            return "TIME_FRAME_H1"
        elif frequency <= datetime.timedelta(hours=2):
            return "TIME_FRAME_H2"
        elif frequency <= datetime.timedelta(hours=4):
            return "TIME_FRAME_H4"
        elif frequency <= datetime.timedelta(hours=8):
            return "TIME_FRAME_H8"
        elif frequency <= datetime.timedelta(days=1):
            return "TIME_FRAME_D"
        elif frequency <= datetime.timedelta(weeks=1):
            return "TIME_FRAME_W"
        elif frequency <= datetime.timedelta(days=31):
            return "TIME_FRAME_MN"
        elif frequency <= datetime.timedelta(days=95):
            return "TIME_FRAME_QR"

        raise ValueError(f"Unsupported frequency for Yahoo Finance {frequency}")

    async def fetch_historical_lime_trader_data(self,
                                                channel: grpc.aio.Channel,
                                                date_from: datetime.datetime,
                                                date_to: datetime.datetime,
                                                symbol: str,
                                                frequency: datetime.timedelta,
                                                ) -> tuple[pl.DataFrame, float, float]:
        duration_start = time.time()
        token = await self.get_token()

        stub = marketdata_service_pb2_grpc.MarketDataServiceStub(channel)
        metadata = [('authorization', token)]
        if "@" in symbol:
            ticker, mic = symbol.split("@")
        else:
            ticker, mic = symbol, "XNGS"
        timestamp_from = Timestamp()
        timestamp_to = Timestamp()
        timestamp_from.FromDatetime(date_from)
        timestamp_to.FromDatetime(date_to)
        interval = interval_pb2.Interval(start_time=timestamp_from, end_time=timestamp_to)

        bars_request = marketdata_service_pb2.BarsRequest(
            timeframe=self._get_timeframe(frequency=frequency),
            interval=interval)
        bars_request.symbol = f"{ticker}@{mic}"
        self._logger.info(f"Fetching market data for symbol {symbol} - {date_from} to {date_to}",
                          symbol=symbol, start_date=date_from, end_date=date_to)
        total_requests_time = 0
        request_start = time.time()
        response = await stub.Bars(bars_request, metadata=metadata)
        duration = time.time() - request_start
        total_requests_time += duration

        rows = [
            {
                "date": datetime.datetime.fromtimestamp(candle.timestamp.seconds,
                                          tz=date_from.tzinfo),
                "open": float(candle.open.value),
                "high": float(candle.high.value),
                "low": float(candle.low.value),
                "close": float(candle.close.value),
                "volume": int(float(candle.volume.value)),
                "exchange": mic,
                "exchange_country": "US",
                "price": float(candle.close.value),
                "symbol": ticker,
            } for candle in response.bars
        ]
        self._logger.info(
            f"Fetched market data for symbol {symbol} - {date_from} to {date_to} in {duration:.2f}",
            symbol=symbol, start_date=date_from, end_date=date_to, duration=duration)

        try:

            df = pl.DataFrame(rows, schema=[("open", pl.Float64()), ("close", pl.Float64()),
                                            ("price", pl.Float64()),
                                            ("high", pl.Float64()), ("low", pl.Float64()),
                                            ("volume", pl.Float64()),
                                            ("date", pl.Datetime(time_zone=date_from.tzinfo)), ("exchange", pl.String),
                                            ("exchange_country", pl.String), ("symbol", pl.String)
                                            ])
            duration_total = time.time() - duration_start

            self._logger.info(
                f"Retrieved {len(rows)} candles for {symbol} in {duration_total:.2f}s. "
                f"Total requests time: {total_requests_time:.2f}s",
                total_duration=duration_total, requests_duration=total_requests_time
            )

            return df, duration_total, total_requests_time
        except grpc.RpcError as e:
            self._logger.exception(f"Failed to get day candles for {symbol}")
            raise

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:
        async def fetch_historical(symbol: str, start_date: datetime.datetime,
                                   end_date: datetime.datetime) -> tuple[pl.DataFrame | None, float, float]:
            try:
                credentials = grpc.ssl_channel_credentials()
                async with grpc.aio.secure_channel(self._server_url, credentials) as channel:

                    result, duration, requests_time = await self.fetch_historical_lime_trader_data(channel=channel,
                                                                                                   date_from=start_date,
                                                                                                   date_to=end_date,
                                                                                                   symbol=symbol,
                                                                                                   frequency=frequency)
                    return result, duration, requests_time
            except Exception as e:
                self._logger.exception(
                    f"Exception fetching historical data for symbol {symbol}, date_from={start_date}, date_to={end_date}. Skipping."
                )
                return None, 0, 0

        total_days = (date_to - date_from).days
        final = pl.DataFrame()

        with progressbar(length=len(symbols) * total_days, label="Downloading historical data from Lime Trader",
                         file=sys.stdout) as pbar:

            if frequency >= datetime.timedelta(days=1):
                maximum_batch = datetime.timedelta(days=7200)
            elif frequency >= datetime.timedelta(hours=1):
                maximum_batch = datetime.timedelta(days=365)
            elif frequency >= datetime.timedelta(minutes=1):
                maximum_batch = datetime.timedelta(days=180)
            elif frequency >= datetime.timedelta(seconds=1):
                maximum_batch = datetime.timedelta(days=30)

            tasks = []
            batch_start_date = date_from
            while batch_start_date < date_to:

                batch_end_date = batch_start_date + maximum_batch
                if batch_end_date > date_to:
                    batch_end_date = date_to

                tasks.extend(
                    fetch_historical(
                        symbol=symbol, start_date=batch_start_date, end_date=batch_end_date
                    ) for symbol in symbols
                )
                batch_start_date = batch_end_date

            total_duration = 0
            total_requests_duration = 0
            res = await asyncio.gather(*tasks)

            for item in res:
                df, duration, requests_duration = item
                total_duration += duration
                total_requests_duration += requests_duration
                pbar.update(total_days)
                if df is None:
                    continue
                final = pl.concat([final, df])
            self._logger.info(
                f"Retrieved {len(final)} candles for {symbols} in {total_duration:.2f}s. "
                f"Total requests time: {total_requests_duration:.2f}s",
                total_duration=total_duration, requests_duration=total_requests_duration
            )

        return final

    @classmethod
    def from_env(cls) -> Self:
        token = os.environ.get("GRPC_TOKEN", None)
        server_url = os.environ.get("GRPC_SERVER_URL")
        maximum_threads = os.environ.get("GRPC_MAXIMUM_THREADS", None)
        if token is None:
            raise ValueError("Missing GRPC_TOKEN environment variable.")
        return cls(server_url=server_url, authorization_token=token, maximum_threads=maximum_threads)
