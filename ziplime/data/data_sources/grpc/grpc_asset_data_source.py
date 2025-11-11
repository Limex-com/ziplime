import datetime
import multiprocessing
import os
from typing import Self

import aiocache
import grpc
import structlog

import polars as pl
from aiocache import Cache

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.equity_symbol_mapping import EquitySymbolMapping
from ziplime.assets.models.exchange_info import ExchangeInfo
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.data_sources.grpc.grpc_stubs.grpc.tradeapi.v1.auth import auth_service_pb2_grpc, auth_service_pb2
from ziplime.data.data_sources.grpc.grpc_stubs.grpc.tradeapi.v1.assets import assets_service_pb2_grpc, \
    assets_service_pb2


class GrpcAssetDataSource(AssetDataSource):
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

    async def get_assets(self, **kwargs) -> list[Asset]:
        request = assets_service_pb2.AssetsRequest()
        token = await self.get_token()
        metadata = [('authorization', token)]
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = assets_service_pb2_grpc.AssetsServiceStub(channel)
            response_stream = await stub.Assets(request, metadata=metadata)

            self._logger.info(f"Got {len(response_stream.assets)} assets from GRPC asset data source.")

        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

        equities = {}
        for asset in response_stream.assets:
            if asset.ticker not in equities:
                equities[asset.ticker] =  Equity(
                        asset_name=asset.symbol,
                            symbol_mapping={
                                asset.mic: EquitySymbolMapping(
                                    symbol=asset.ticker,
                                    exchange_name=asset.mic,
                                    start_date=asset_start_date,
                                    end_date=asset_end_date,
                                    company_symbol="",
                                    share_class_symbol=""
                                )
                                # "LIME": EquitySymbolMapping(
                                #     symbol=asset.symbol,
                                #     exchange_name="LIME",
                                #     start_date=asset_start_date,
                                #     end_date=asset_end_date,
                                #     company_symbol="",
                                #     share_class_symbol=""
                                # )
                            },
                            sid=None,
                            start_date=asset_start_date,
                            end_date=asset_end_date,
                            auto_close_date=asset_end_date,
                            first_traded=asset_start_date,
                            mic=asset.mic
                        )
            else:
                equities[asset.ticker].symbol_mapping[asset.mic] =EquitySymbolMapping(
                                    symbol=asset.ticker,
                                    exchange_name=asset.mic,
                                    start_date=asset_start_date,
                                    end_date=asset_end_date,
                                    company_symbol="",
                                    share_class_symbol=""
                                )

        return list(equities.values())

    async def get_exchanges(self, **kwargs) ->list[ExchangeInfo]:
        request = assets_service_pb2.ExchangesRequest()
        token = await self.get_token()
        metadata = [('authorization', token)]
        credentials = grpc.ssl_channel_credentials()

        async with grpc.aio.secure_channel(self._server_url, credentials) as channel:
            stub = assets_service_pb2_grpc.AssetsServiceStub(channel)
            response_stream = await stub.Exchanges(request, metadata=metadata)
            exchanges = [ExchangeInfo(exchange=exchange.mic, canonical_name=exchange.name, country_code="US")
                         for exchange in response_stream.exchanges]
            self._logger.info(f"Got {len(exchanges)} exchanges from GRPC asset data source.")
        return exchanges

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

    @classmethod
    def from_env(cls) -> Self:
        token = os.environ.get("GRPC_TOKEN", None)
        server_url = os.environ.get("GRPC_SERVER_URL")
        maximum_threads = os.environ.get("GRPC_MAXIMUM_THREADS", None)
        if token is None:
            raise ValueError("Missing GRPC_TOKEN environment variable.")
        return cls(server_url=server_url, authorization_token=token, maximum_threads=maximum_threads)
