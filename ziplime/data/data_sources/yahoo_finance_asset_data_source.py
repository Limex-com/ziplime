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
import yfinance as yf
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.services.data_bundle_source import DataBundleSource


class YahooFinanceAssetDataSource(AssetDataSource):
    def __init__(self, maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_assets(self, symbols: list[str], **kwargs) -> pl.DataFrame:
        assets = yf.Tickers(' '.join(symbols))
        return assets

    async def search_assets(self, query: str, **kwargs) -> pl.DataFrame:
        assets = yf.Lookup(query=query).all
        return assets

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

