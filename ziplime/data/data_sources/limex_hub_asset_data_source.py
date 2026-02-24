import datetime
import multiprocessing
import os
from typing import Self

import limexhub
import structlog

import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.equity_symbol_mapping import EquitySymbolMapping
from ziplime.assets.models.exchange_info import ExchangeInfo
from ziplime.data.data_sources.asset_data_source import AssetDataSource


class LimexHubAssetDataSource(AssetDataSource):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None):
        super().__init__()
        self._limex_api_key = limex_api_key
        self._logger = structlog.get_logger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_assets(self, **kwargs) -> list[Asset]:
        assets = self._limex_client.instruments()

        assets_df = pl.from_dataframe(assets)
        assets_df = assets_df.rename({
            "ticker": "symbol"
        })
        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

        equities = [
            Equity(
                asset_name=asset["symbol"],
                symbol_mapping={
                    "LIME": EquitySymbolMapping(
                        symbol=asset["symbol"],
                        exchange_name="LIME",
                        start_date=asset_start_date,
                        end_date=asset_end_date,
                        company_symbol="",
                        share_class_symbol=""
                    )
                },
                sid=None,
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                mic="LIME"
            ) for asset in assets_df.iter_rows(named=True)
        ]

        return equities

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        exchanges = [ExchangeInfo(exchange="LIME", canonical_name="LIME", country_code="US")]
        return exchanges

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
