import datetime

import aiocache
import pandas as pd
import polars as pl
from aiocache import Cache

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.models.dividend import Dividend
from ziplime.assets.repositories.adjustments_repository import AdjustmentRepository
from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.trading.entities.trading_pair import TradingPair


class AssetService:

    def __init__(self, asset_repository: AssetRepository, adjustments_repository: AdjustmentRepository):
        self._asset_repository = asset_repository
        self._adjustments_repository = adjustments_repository

    async def save_equities(self, equities: list[Equity]) -> None:
        await self._asset_repository.save_equities(equities=equities)

    async def save_symbol_universe(self, symbol_universe: SymbolsUniverse) -> None:
        await self._asset_repository.save_symbol_universe(symbol_universe=symbol_universe)

    async def save_commodities(self, commodities: list[Commodity]) -> None:
        await self._asset_repository.save_commodities(commodities=commodities)

    async def save_currencies(self, currencies: list[Currency]) -> None:
        await self._asset_repository.save_currencies(currencies=currencies)

    async def save_exchange_assets(self, exchange_assets: list[ExchangeAsset]) -> None:
        await self._asset_repository.save_exchange_assets(exchange_assets=exchange_assets)

    async def save_exchanges(self, exchanges: list[ExchangeInfo]) -> None:
        return await self._asset_repository.save_exchanges(exchanges=exchanges)

    async def save_trading_pairs(self, trading_pairs: list[TradingPair]) -> None:
        ...

    async def get_asset_by_sid(self, sid: int) -> ExchangeAsset | None:
        return await self._asset_repository.get_asset_by_sid(sid=sid)

    async def get_assets_by_ids(self, ids: list[int]) -> list[ExchangeAsset]:
        return await self._asset_repository.get_assets_by_ids(ids=ids)

    async def get_equity_by_symbol(self, symbol: str, mic: str) -> Equity | None:
        return await self._asset_repository.get_equity_by_symbol(symbol=symbol,
                                                                 mic=mic)

    async def get_equities_by_symbols(self, symbols: list[str]) -> list[Equity]:
        return await self._asset_repository.get_equities_by_symbols(symbols=symbols)

    async def get_exchange_equities_by_symbols(self, symbols: list[AssetSymbol]) -> list[ExchangeAsset]:
        return await self._asset_repository.get_exchange_equities_by_symbols(symbols=symbols)

    async def get_equities_by_isins(self, isins: list[str]) -> list[Equity]:
        return await self._asset_repository.get_equities_by_isins(isins=isins)

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_exchange_asset_by_symbol(self, symbol: AssetSymbol, asset_type: AssetType) -> ExchangeAsset | None:
        return await self._asset_repository.get_exchange_asset_by_symbol(
            symbol=symbol,
            asset_type=asset_type
        )

    async def get_exchange_assets_by_symbols(self, symbols: list[AssetSymbol], asset_type: AssetType) -> list[
        ExchangeAsset | None]:
        return [await self.get_exchange_asset_by_symbol(
            symbol=symbol,
            asset_type=asset_type
        ) for symbol in symbols]

    async def get_futures_contract_by_symbol(self, symbol: str, mic: str) -> FuturesContract | None:
        return await self._asset_repository.get_futures_contract_by_symbol(symbol=symbol,
                                                                           mic=mic)

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_currency_by_symbol(self, symbol: str, mic: str) -> Currency | None:
        return await self._asset_repository.get_currency_by_symbol(symbol=symbol,
                                                                   mic=mic)

    async def get_commodity_by_symbol(self, symbol: str, mic: str) -> Commodity | None:
        return await self._asset_repository.get_commodity_by_symbol(symbol=symbol,
                                                                    mic=mic)

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[Dividend]:
        return await self._adjustments_repository.get_stock_dividends(sid=sid,
                                                                      trading_days=trading_days)

    async def get_splits(self, assets: frozenset[ExchangeAsset], dt: datetime.date):
        return await self._adjustments_repository.get_splits(assets=assets, dt=dt)

    async def get_symbols_universe(self, name: str, dt: datetime.date) -> SymbolsUniverse | None:
        return await self._asset_repository.get_symbols_universe(name=name, dt=dt)

    async def lifetimes(self, dates: pd.DatetimeIndex, include_start_date: bool, country_codes: list[str]):
        # normalize to a cache-key so that we can memoize results.
        lifetimes = await self._asset_repository.lifetimes(dates=dates, include_start_date=include_start_date,
                                                           country_codes=country_codes)

        raw_dates = dates.view('int64') // 10 ** 9
        if include_start_date:
            mask = lifetimes.start[None, :] <= raw_dates[:, None]
        else:
            mask = lifetimes.start[None, :] < raw_dates[:, None]
        mask &= raw_dates[:, None] <= lifetimes.end[None, :]
        return pd.DataFrame(mask, index=dates, columns=lifetimes.sid)

    async def asset_lifetimes(self, assets: list[ExchangeAsset], dates: pd.DatetimeIndex, include_start_date: bool):
        # normalize to a cache-key so that we can memoize results.
        lifetimes = await self._asset_repository.asset_lifetimes(dates=dates, include_start_date=include_start_date,
                                                                 assets=assets)

        raw_dates = dates.view('int64') // 10 ** 9
        if include_start_date:
            mask = lifetimes.start[None, :] <= raw_dates[:, None]
        else:
            mask = lifetimes.start[None, :] < raw_dates[:, None]
        mask &= raw_dates[:, None] <= lifetimes.end[None, :]
        return pd.DataFrame(mask, index=dates, columns=lifetimes.sid)

    async def retrieve_all(self, sids: list[int], default_none: bool = False):
        return await self._asset_repository.retrieve_all(sids=sids, default_none=default_none)

    async def load_pricing_adjustments(self, columns, dates, assets):
        return await self._adjustments_repository.load_pricing_adjustments(columns=columns, dates=dates, assets=assets)
