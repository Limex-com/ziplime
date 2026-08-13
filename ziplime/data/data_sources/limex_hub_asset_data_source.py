import datetime
import multiprocessing
import os
from typing import Self

import limexhub
import structlog

import polars as pl

from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.entities.symbols_universe_asset import SymbolsUniverseAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.exchanges.exchange import Exchange


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

    async def get_assets(self, exchanges: list[ExchangeInfo], **kwargs) -> AssetsImport:
        exchanges_by_code = {exchange.mic: exchange for exchange in exchanges}
        assets_df = self._fetch_all_instruments()
        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

        assets_df = assets_df.with_columns(pl.lit('USD').alias('currency'))

        equities = [
            Equity(
                asset_name=asset["ticker"],
                id=None,
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                isin=asset["isin"]
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

        # exchange_currencies = [
        #     ExchangeAsset(
        #         sid=None,
        #         symbol=currency.asset_name,
        #         exchange=exchange,
        #         start_date=asset_start_date,
        #         end_date=asset_end_date,
        #         auto_close_date=asset_end_date,
        #         first_traded=asset_start_date,
        #         external_id=currency.asset_name,
        #         asset=currency
        #     )
        #     for exchange in exchanges
        #     for currency in currencies
        # ]

        exchange_assets = [
            ExchangeAsset(
                sid=None,
                symbol=asset_df["ticker"],
                exchange=exchanges_by_code.get(asset_df["mic"], ExchangeInfo(mic=asset_df["mic"], name=asset_df["mic"],
                                                                             canonical_name=asset_df["mic"],
                                                                             country_code="US")),
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                asset=asset,
                external_id=asset_df["id"],
                quote=currencies[0]
            )
            for asset, asset_df in zip(equities, assets_df.iter_rows(named=True))
        ]

        # exchange_assets.extend(exchange_currencies)
        return AssetsImport(
            currencies=currencies, equities=equities, exchange_assets=exchange_assets
        )

    INSTRUMENTS_FILTERS = [
        'XNGS@EQUITIES',
        'XNYS@EQUITIES',
        'ARCX@EQUITIES',
        'ARCX@FUNDS',
        'XNMS@FUNDS',
        'BATS@FUNDS',
        'MISX@EQUITIES',
        'MISX@FUNDS',
        'MISX@INDICES',
        '_BNCC@CURRENCIES',
        'RTSX@FUTURES',
    ]

    def _fetch_all_instruments(self) -> pl.DataFrame:
        frames = []
        for f in self.INSTRUMENTS_FILTERS:
            self._logger.info("fetching instruments", filter=f)
            raw = self._limex_client.instruments(f)
            if raw is not None and len(raw) > 0:
                frames.append(pl.from_dataframe(raw))
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal").unique(subset=["id"])

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        assets_df = self._fetch_all_instruments()
        exchanges = [
            ExchangeInfo(mic=mic, name=mic, canonical_name=mic, country_code="US")
            for mic in assets_df["mic"].unique().to_list() if mic is not None
        ]
        return exchanges

    async def get_symbol_universe(self, asset_service: AssetService, symbol_universe_name: str) -> SymbolsUniverse:
        assets = self._limex_client.constituents(universe=symbol_universe_name)
        symbols = list(set(assets["ticker"]))
        isins = list(set(assets["isin"]))
        equities = await asset_service.get_equities_by_isins(isins=isins)

        assets_by_isin = {asset.isin: asset for asset in equities}

        universe_assets = []
        for row in assets.itertuples():
            asset = assets_by_isin.get(row.isin, None)
            if asset is None:
                self._logger.warning(f"Asset {row.ticker}-{row.isin} does not exist in assets database. Skipping.")
                continue
            universe_assets.append(SymbolsUniverseAsset(
                symbol_universe_name=symbol_universe_name,
                start_date=datetime.date.fromisoformat(row.start_date),
                end_date=datetime.date.fromisoformat(row.end_date),
                asset=asset,
                ratio=None
            ))

        universe = SymbolsUniverse(
            name=symbol_universe_name,
            universe_type="index",
            symbol=symbol_universe_name,
            assets=universe_assets
        )
        return universe

    @classmethod
    def from_env(cls) -> Self:
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return cls(limex_api_key=limex_hub_key, maximum_threads=maximum_threads)
