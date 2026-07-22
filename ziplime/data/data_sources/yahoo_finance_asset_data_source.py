import datetime
import multiprocessing
import string
import time
from typing import Self

import structlog

import polars as pl
import yfinance as yf
from yfinance.exceptions import YFException

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.data_sources.yahoo_finance_constants import YAHOO_EXCHANGE_MAP


class YahooFinanceAssetDataSource(AssetDataSource):
    def __init__(self, maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_assets(self, exchanges: list[ExchangeInfo], **kwargs) -> list[ExchangeAsset]:
        exchanges_by_code = {exchange.mic: exchange for exchange in exchanges}
        lookup_letters = list(string.ascii_lowercase)
        result_df = None
        for letter in lookup_letters:
            try:
                res = yf.Lookup(letter.upper()).get_stock(count=1000)
            except YFException as e:
                time.sleep(20)
                res = yf.Lookup(letter.upper()).get_stock(count=1000)

            yahoo_df = pl.from_pandas(res, include_index=True).select("symbol", "exchange").with_columns(
                pl.col("exchange").replace({k: v["mic"]for k,v in  YAHOO_EXCHANGE_MAP.items()}).alias("mic")
            )
            if result_df is None:
                result_df = yahoo_df
            else:
                result_df.extend(yahoo_df)
            self._logger.info(f"Fetched {len(result_df)} from YahooFinance with lookup for {letter.upper()}")
            print(f"Fetched {len(result_df)} from YahooFinance with lookup for {letter.upper()}")
            time.sleep(1)
        assets_df = result_df.unique()

        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

        assets_df = assets_df.with_columns(pl.lit('USD').alias('currency'))

        equities = [
            Equity(
                asset_name=asset["symbol"],
                id=None,
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                isin=""
            ) for asset in assets_df.iter_rows(named=True)
        ]

        currencies = [Currency(
            asset_name=currency,
            id=None,
            start_date=asset_start_date,
            end_date=asset_end_date,
            auto_close_date=asset_end_date,
            first_traded=asset_start_date,
            isin=None
        ) for currency in assets_df["currency"].unique()]
        exchange_currencies = [
            ExchangeAsset(
                sid=None,
                symbol=currency.asset_name,
                exchange=exchange,
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                external_id=currency.asset_name,
                asset=currency
            )
            for exchange in exchanges
            for currency in currencies
        ]

        exchange_assets = [
            ExchangeAsset(
                sid=None,
                symbol=asset_df["symbol"],
                exchange=exchanges_by_code.get(asset_df["mic"], ExchangeInfo(mic=asset_df["mic"], name=asset_df["mic"],
                                                                             canonical_name=asset_df["mic"],
                                                                             country_code="US")),
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                asset=asset,
                external_id=""
            )
            for asset, asset_df in zip(equities, assets_df.iter_rows(named=True))
        ]

        exchange_assets.extend(exchange_currencies)
        return exchange_assets

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        exchanges = [
            ExchangeInfo(mic=exchange["mic"], name=exchange["name"], canonical_name=exchange["name"],
                         country_code=exchange["country_code"])
            for exchange in YAHOO_EXCHANGE_MAP.values()
        ]
        exchanges.append(
            ExchangeInfo(mic="", name="Unknown", canonical_name="Unknown",
                         country_code="US"
                         )
        )
        return exchanges

    @classmethod
    def from_env(cls) -> Self:
        return cls()
