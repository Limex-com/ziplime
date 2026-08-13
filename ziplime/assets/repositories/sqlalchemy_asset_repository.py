import datetime
from collections import deque
from functools import partial
from operator import attrgetter
from pathlib import Path
from typing import Any, Self
import pathlib

import aiocache
import pandas as pd
import sqlalchemy as sa
from aiocache import cached, Cache
from alembic import config, command
from sqlalchemy import Table, select, tuple_
from sqlalchemy.orm import selectinload
from toolz import (
    concat,
    merge,
    partition_all,
)

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.split import Split
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.entities.symbols_universe_asset import SymbolsUniverseAsset
from ziplime.assets.models.asset_router import AssetRouter
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.models.commodity_model import CommodityModel
from ziplime.assets.models.currency_model import CurrencyModel
from ziplime.assets.models.divident_payout_model import DividendPayoutModel
from ziplime.assets.models.equity_model import EquityModel
from ziplime.assets.models.exchange_asset_model import ExchangeAssetModel
from ziplime.assets.models.futures_contract_model import FuturesContractModel
from ziplime.assets.models.split_model import SplitModel
from ziplime.assets.models.symbols_universe import SymbolsUniverseModel
from ziplime.assets.models.symbols_universe_asset import SymbolsUniverseAssetModel
from ziplime.trading.models.trading_pair import TradingPair
from ziplime.core.db.base_model import BaseModel
from ziplime.errors import (
    EquitiesNotFound,
    FutureContractsNotFound,
    MultipleSymbolsFound,
    SameSymbolUsedAcrossCountries,
    SidsNotFound,
    SymbolNotFound,
)
from ziplime.utils.functional import invert
from ziplime.utils.numpy_utils import as_column
from ziplime.utils.sqlite_utils import group_into_chunks, SQLITE_MAX_VARIABLE_NUMBER

from ziplime.assets.models.exchange_info_model import ExchangeInfoModel

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from ziplime.assets.models.asset_model import AssetModel
from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES, OrderedContracts, ADJUSTMENT_STYLES
from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.assets.utils import _convert_asset_timestamp_fields, _filter_future_kwargs, \
    _filter_equity_kwargs, _encode_continuous_future_sid, Lifetimes, \
    build_grouped_ownership_map, OwnershipPeriod, SYMBOL_COLUMNS, split_delimited_symbol


class SqlAlchemyAssetRepository(AssetRepository):
    """An AssetFinder is an interface to a database of Asset metadata written by
    an ``AssetDBWriter``.

    This class provides methods for looking up assets by unique integer id or
    by symbol.  For historical reasons, we refer to these unique ids as 'sids'.

    Parameters
    ----------
    engine : str or SQLAlchemy.engine
        An engine with a connection to the asset database to use, or a string
        that can be parsed by SQLAlchemy as a URI.
    future_chain_predicates : dict
        A dict mapping future root symbol to a predicate function which accepts
    a contract as a parameter and returns whether or not the contract should be
    included in the chain.

    See Also
    --------
    :class:`ziplime.assets.repositories.AssetsRepository`
    """

    def __init__(self, db_url: str, future_chain_predicates):
        self.db_url = db_url
        self._asset_cache = {}
        self._asset_type_cache = {}
        self._caches = (self._asset_cache, self._asset_type_cache)

        self._future_chain_predicates = (
            future_chain_predicates if future_chain_predicates is not None else {}
        )
        self._ordered_contracts = {}

        # Populated on first call to `lifetimes`.
        self._asset_lifetimes = {}
        self._cached_assets: dict[int, Asset] = {}
        self.migrate()
        self.engine = create_async_engine(self.db_url, pool_pre_ping=True, pool_size=20)
        self.session_maker = async_sessionmaker(autocommit=False, autoflush=True, bind=self.engine, class_=AsyncSession,
                                                expire_on_commit=False)

    async def add_all_and_commit(self, models: list[BaseModel]):
        async with self.session_maker() as session:
            session.add_all(models)
            await session.commit()

    async def save_trading_pairs(self, trading_pairs: list[TradingPair]) -> None:
        await self.add_all_and_commit(trading_pairs)

    async def save_asset_routers(self, asset_routers: list[AssetRouter]) -> None:
        await self.add_all_and_commit(asset_routers)

    def _currency_model_to_currency(self, currency_model: CurrencyModel) -> Currency:
        return Currency(
            id=currency_model.id,
            asset_name=currency_model.asset_name,
            start_date=currency_model.start_date,
            first_traded=currency_model.first_traded,
            end_date=currency_model.end_date,
            auto_close_date=currency_model.auto_close_date,
            isin=currency_model.isin
        )

    def _equity_model_to_equity(self, equity_model: EquityModel) -> Equity:
        return Equity(
            id=equity_model.id,
            asset_name=equity_model.asset_name,
            start_date=equity_model.start_date,
            first_traded=equity_model.first_traded,
            end_date=equity_model.end_date,
            auto_close_date=equity_model.auto_close_date,
            isin=equity_model.isin
        )

    async def save_currencies(self, currencies: list[Currency]) -> list[Currency]:
        assets_db = []
        asset_routers = []
        async with self.session_maker() as session:
            for currency in currencies:
                asset_router = AssetRouter(
                    id=currency.id,
                    asset_type=AssetType.CURRENCY.value
                )
                asset_routers.append(asset_router)
                session.add(asset_router)
                await session.commit()
                asset_db = CurrencyModel(
                    id=asset_router.id,
                    start_date=currency.start_date,
                    first_traded=currency.first_traded,
                    end_date=currency.end_date,
                    asset_name=currency.asset_name,
                    auto_close_date=currency.auto_close_date,
                )
                session.add(asset_db)
                await session.commit()
                assets_db.append(asset_db)

        return [self._currency_model_to_currency(currency_model=c) for c in assets_db]

    async def save_symbol_universe(self, symbol_universe: SymbolsUniverse):
        async with self.session_maker() as session:
            symbol_universe_model = SymbolsUniverseModel(symbol=symbol_universe.symbol,
                                                         universe_type=symbol_universe.universe_type,
                                                         name=symbol_universe.name,
                                                         )
            session.add(symbol_universe_model)
            await session.commit()
            await session.refresh(symbol_universe_model)
            symbol_universe_assets = [
                SymbolsUniverseAssetModel(symbol_universe_name=symbol_universe_model.name,
                                          start_date=asset.start_date,
                                          end_date=asset.end_date,
                                          asset_id=asset.asset.id)
                for asset in symbol_universe.assets
            ]
            session.add_all(symbol_universe_assets)
            await session.commit()

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_exchange_by_mic(self, mic: str) -> ExchangeInfo | None:
        async with self.session_maker() as session:
            q = select(ExchangeInfoModel).where(ExchangeInfoModel.mic == mic)
            exchange = (await session.execute(q)).scalar_one_or_none()
        if exchange is None:
            return None
        exchange_info = ExchangeInfo(
            mic=exchange.mic,
            country_code=exchange.country_code,
            canonical_name=exchange.canonical_name,
            name=exchange.name
        )
        return exchange_info

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_exchanges_by_country_codes(self, country_codes: frozenset[str]) -> list[ExchangeInfo]:
        async with self.session_maker() as session:
            q = select(ExchangeInfoModel).where(ExchangeInfoModel.country_code.in_(country_codes))
            exchanges = list((await session.execute(q)).scalars())

        return [ExchangeInfo(
            mic=exchange.mic,
            country_code=exchange.country_code,
            canonical_name=exchange.canonical_name,
            name=exchange.name
        ) for exchange in exchanges]

    async def save_equities(self, equities: list[Equity]) -> list[Equity]:
        assets_db = []
        asset_routers = []
        # symbol_mappings = []
        for equity in equities:
            asset_router = AssetRouter(
                id=equity.id,
                asset_type=AssetType.EQUITY.value
            )
            asset_routers.append(asset_router)
        async with self.session_maker() as session:
            session.add_all(asset_routers)
            await session.commit()

        for i, equity in enumerate(equities):
            asset_db = EquityModel(
                id=asset_routers[i].id,
                start_date=equity.start_date,
                first_traded=equity.first_traded,
                end_date=equity.end_date,
                asset_name=equity.asset_name,
                auto_close_date=equity.auto_close_date,
                isin=equity.isin
            )
            assets_db.append(asset_db)
        async with self.session_maker() as session:
            session.add_all(assets_db)
            await session.commit()

        return [self._equity_model_to_equity(equity_model=eq) for eq in assets_db]

    async def save_exchanges(self, exchanges: list[ExchangeInfo]) -> None:
        exchange_models = [
            ExchangeInfoModel(
                mic=exchange.mic,
                name=exchange.name,
                canonical_name=exchange.canonical_name,
                country_code=exchange.country_code
            ) for exchange in exchanges
        ]

        await self.add_all_and_commit(exchange_models)

    async def save_exchange_assets(self, exchange_assets: list[ExchangeAsset]) -> list[ExchangeAsset]:
        all_assets = await self.get_all_assets()

        asset_keys = {
            (type(asset), asset.isin, asset.asset_name): asset
            for asset in all_assets.values()
        }

        exchange_assets_db = [
            ExchangeAssetModel(
                # exchange="",
                external_id=exchange_asset.external_id,
                start_date=exchange_asset.start_date,
                end_date=exchange_asset.end_date,
                symbol=exchange_asset.symbol,
                asset_id=asset_keys[
                    (type(exchange_asset.asset), exchange_asset.asset.isin, exchange_asset.asset.asset_name)].id,
                quote_id=asset_keys[
                    (type(exchange_asset.quote), exchange_asset.quote.isin, exchange_asset.quote.asset_name)].id,
                mic=exchange_asset.mic,
                first_traded=exchange_asset.first_traded,
                sid=None,
                auto_close_date=exchange_asset.auto_close_date,
            )
            for exchange_asset in exchange_assets
        ]
        await self.add_all_and_commit(exchange_assets_db)

    async def save_dividends(self, dividends: list[DividendPayout]) -> list[DividendPayout]:
        all_assets = await self.get_all_assets()
        dividends_db = [DividendPayoutModel(
            asset_id=d.asset.id,
            ex_date=d.pay_date,
            declared_date=d.declared_date,
            record_date=d.record_date,
            pay_date=d.pay_date,
            amount=d.amount
        ) for d in dividends]

        await self.add_all_and_commit(dividends_db)

    async def save_splits(self, splits: list[Split]) -> list[Split]:
        splits_db = [SplitModel(
            asset_id=s.asset.id,
            effective_date=s.effective_date,
            ratio=s.ratio
        ) for s in splits]

        await self.add_all_and_commit(splits_db)

    # async def save_equity_symbol_mappings(self, equity_symbol_mappings: list[EquitySymbolMappingModel]) -> None:
    #     await self.add_all_and_commit(equity_symbol_mappings)

    @cached(cache=Cache.MEMORY)
    async def get_all_assets(self) -> dict[int, Asset]:
        if self._cached_assets:
            return self._cached_assets

        async with self.session_maker() as session:
            q_equities = select(EquityModel).options(selectinload(EquityModel.asset_router))
            equities = list((await session.execute(q_equities)).scalars().all())

            q_futures_contracts = select(FuturesContractModel).options(selectinload(FuturesContractModel.asset_router))
            futures_contracts = list((await session.execute(q_futures_contracts)).scalars().all())

            q_currencies = select(CurrencyModel).options(selectinload(CurrencyModel.asset_router))
            currencies = list((await session.execute(q_currencies)).scalars().all())

            q_commodities = select(CommodityModel).options(selectinload(CommodityModel.asset_router))
            commodities = list((await session.execute(q_commodities)).scalars().all())
        res = {}
        for asset in equities:
            res[asset.id] = self._equity_model_to_equity(equity_model=asset)
        for asset in futures_contracts:
            # Map to your pure FuturesContract domain entity...
            pass
        for asset in currencies:
            # Map to your pure Currency domain entity...
            res[asset.id] = self._currency_model_to_currency(currency_model=asset)
        for asset in commodities:
            # Map to your pure Commodity domain entity...
            pass
        self._cached_assets = res
        return res

    @cached(cache=Cache.MEMORY)
    async def get_all_universes(self) -> dict[str, SymbolsUniverse]:
        async with self.session_maker() as session:
            q = select(SymbolsUniverseModel).options(
                selectinload(SymbolsUniverseModel.assets)
            )
            universes = list((await session.execute(q)).scalars())
            assets_by_sids = {asset.id: asset for asset in await self.get_assets_by_ids(
                ids=list(set(asset.asset_id for universe in universes for asset in universe.assets)))}
            res = {
                universe.symbol:
                    SymbolsUniverse(
                        assets=[
                            SymbolsUniverseAsset(
                                symbol_universe_name=universe.name,
                                asset=assets_by_sids[asset.asset_id],
                                start_date=asset.start_date,
                                end_date=asset.end_date,
                                ratio=asset.ratio
                            )
                            for asset in universe.assets

                        ],
                        universe_type=universe.universe_type,
                        symbol=universe.symbol,
                        name=universe.name
                    )
                for universe in universes
            }
        return res

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_exchange_asset_by_symbol(self, symbol: AssetSymbol, asset_type: AssetType, ) -> ExchangeAsset | None:
        match asset_type:
            case AssetType.EQUITY:
                return await self.get_exchange_equity_by_symbol(symbol=symbol)
            case AssetType.FUTURES_CONTRACT:
                return await self.get_exchange_futures_contract_by_symbol(symbol=symbol)
            case AssetType.CURRENCY:
                return await self.get_exchange_currency_by_symbol(symbol=symbol)
            case _:
                raise ValueError(f"Invalid asset type: {asset_type}")

    async def get_asset_by_sid(self, sid: int) -> AssetModel | None:
        assets_by_sid = await self.get_all_assets()
        return assets_by_sid.get(sid, None)

    async def get_assets_by_ids(self, ids: list[int]) -> list[Asset]:
        assets_by_id = await self.get_all_assets()
        assets = [assets_by_id.get(id, None) for id in ids]
        return assets

    async def get_symbols_universe(self, name: str, dt: datetime.date) -> SymbolsUniverse | None:
        universes_by_symbol = await self.get_all_universes()
        universe = universes_by_symbol.get(name, None)
        if universe is None:
            return universe
        return SymbolsUniverse(
            name=universe.name,
            symbol=universe.symbol,
            assets=[
                asset for asset in universe.assets if asset.start_date <= dt and asset.end_date >= dt
            ],
            universe_type=universe.universe_type
        )

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_currencies_by_symbols(self, symbols: list[str]) -> list[Currency]:
        async with self.session_maker() as session:
            q_currencies = select(CurrencyModel).where(
                CurrencyModel.asset_name.in_(symbols)).options(
                selectinload(CurrencyModel.asset_router))
            assets: list[CurrencyModel] = list((await session.execute(q_currencies)).scalars())

            return [Currency(
                id=asset.id,
                asset_name=asset.asset_name,
                start_date=asset.start_date,
                first_traded=asset.first_traded,
                end_date=asset.end_date,
                auto_close_date=asset.auto_close_date,
                isin=asset.isin
            ) for asset in assets]

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_currency_by_symbol(self, symbol: str) -> Currency | None:
        currencies = await self.get_currencies_by_symbols(symbols=[symbol])
        if currencies:
            return currencies[0]
        return None

    async def get_commodity_by_symbol(self, symbol: str) -> Commodity | None:
        raise NotImplementedError("Not implemented")

    async def get_futures_contract_by_symbol(self, symbol: str, exchange_name: str) -> FuturesContract | None:
        raise NotImplementedError("Not implemented")

    async def get_equities_by_symbols_and_exchange(self, symbols: list[str], exchange_name: str) -> list[Equity]:
        async with self.session_maker() as session:
            q_equity_symbol_mapping = select(EquitySymbolMappingModel).where(
                EquitySymbolMappingModel.exchange == exchange_name,
                EquitySymbolMappingModel.symbol.in_(symbols))

            equity_mappings = (await session.execute(q_equity_symbol_mapping)).scalars()

            q_equities = select(EquityModel).where(
                EquityModel.sid.in_([equity_mapping.sid for equity_mapping in equity_mappings])).options(
                selectinload(EquityModel.asset_router)).options(selectinload(EquityModel.equity_symbol_mappings))
            assets: list[EquityModel] = list((await session.execute(q_equities)).scalars())

            return [Equity(
                sid=asset.sid,
                asset_name=asset.asset_name,
                start_date=asset.start_date,
                first_traded=asset.first_traded,
                end_date=asset.end_date,
                auto_close_date=asset.auto_close_date,
                symbol_mapping={
                    equity_mapping.exchange: EquitySymbolMapping(
                        company_symbol=equity_mapping.company_symbol,
                        symbol=equity_mapping.symbol,
                        exchange_name=equity_mapping.exchange,
                        share_class_symbol=equity_mapping.share_class_symbol,
                        end_date=equity_mapping.end_date,
                        start_date=equity_mapping.start_date
                    )
                    for equity_mapping in asset.equity_symbol_mappings
                },
                mic=asset.mic,
                isin=asset.isin
            ) for asset in assets]

    async def get_equities_by_symbols(self, symbols: list[AssetSymbol]) -> list[Equity]:
        async with self.session_maker() as session:
            q_equity_symbol_mapping = select(EquitySymbolMappingModel).where(
                EquitySymbolMappingModel.symbol.in_(symbols))

            equity_mappings = (await session.execute(q_equity_symbol_mapping)).scalars()

            q_equities = select(EquityModel).where(
                EquityModel.sid.in_([equity_mapping.sid for equity_mapping in equity_mappings])).options(
                selectinload(EquityModel.asset_router)).options(selectinload(EquityModel.equity_symbol_mappings))
            assets: list[EquityModel] = list((await session.execute(q_equities)).scalars())

            return [Equity(
                sid=asset.sid,
                asset_name=asset.asset_name,
                start_date=asset.start_date,
                first_traded=asset.first_traded,
                end_date=asset.end_date,
                auto_close_date=asset.auto_close_date,
                symbol_mapping={
                    equity_mapping.exchange: EquitySymbolMapping(
                        company_symbol=equity_mapping.company_symbol,
                        symbol=equity_mapping.symbol,
                        exchange_name=equity_mapping.exchange,
                        share_class_symbol=equity_mapping.share_class_symbol,
                        end_date=equity_mapping.end_date,
                        start_date=equity_mapping.start_date
                    )
                    for equity_mapping in asset.equity_symbol_mappings
                },
                mic=asset.mic,
                isin=asset.isin
            ) for asset in assets]

    async def get_exchange_currencies_by_symbols(self, symbols: list[AssetSymbol]) -> list[ExchangeAsset]:

        filter_mic = tuple_(ExchangeAssetModel.symbol, ExchangeAssetModel.mic).in_(
            [(s.symbol, s.mic) for s in symbols if s.mic is not None]
        )
        filter_non_mic = ExchangeAssetModel.symbol.in_([s.symbol for s in symbols if s.mic is None])
        async with self.session_maker() as session:
            q = (
                select(ExchangeAssetModel)
                .where(
                    filter_mic | filter_non_mic
                )
                .options(selectinload(ExchangeAssetModel.asset_router))
            ).distinct(ExchangeAssetModel.mic, ExchangeAssetModel.symbol).order_by(
                ExchangeAssetModel.mic, ExchangeAssetModel.symbol, "sid")
            results: list[ExchangeAssetModel] = list((await session.execute(q)).scalars())

            q_currencies = select(CurrencyModel).where(
                CurrencyModel.id.in_([currency_exchange.asset_id for currency_exchange in results])).options(
                selectinload(CurrencyModel.asset_router))
            assets: list[CurrencyModel] = list((await session.execute(q_currencies)).scalars())
            assets_by_id = {asset.id: asset for asset in assets}
            all_assets = await self.get_all_assets()
        return [ExchangeAsset(
            sid=asset.sid,
            start_date=asset.start_date,
            first_traded=asset.first_traded,
            end_date=asset.end_date,
            auto_close_date=asset.auto_close_date,
            symbol=asset.symbol,
            exchange=await self.get_exchange_by_mic(mic=asset.mic),
            asset=Currency(
                first_traded=assets_by_id[asset.asset_id].first_traded,
                auto_close_date=assets_by_id[asset.asset_id].auto_close_date,
                end_date=assets_by_id[asset.asset_id].end_date,
                start_date=assets_by_id[asset.asset_id].start_date,
                isin=assets_by_id[asset.asset_id].isin,
                asset_name=assets_by_id[asset.asset_id].asset_name,
                id=assets_by_id[asset.asset_id].id,
            ),
            quote=all_assets[asset.quote_id],
            external_id=asset.external_id,
        ) for asset in results]

    async def get_exchange_equities_by_symbols(self, symbols: list[AssetSymbol]) -> list[ExchangeAsset]:

        filter_mic = tuple_(ExchangeAssetModel.symbol, ExchangeAssetModel.mic).in_(
            [(s.symbol, s.mic) for s in symbols if s.mic is not None]
        )
        filter_non_mic = ExchangeAssetModel.symbol.in_([s.symbol for s in symbols if s.mic is None])
        async with self.session_maker() as session:
            q = (
                select(ExchangeAssetModel)
                .where(
                    filter_mic | filter_non_mic
                )
                .options(selectinload(ExchangeAssetModel.asset_router))
            ).distinct(ExchangeAssetModel.mic, ExchangeAssetModel.symbol).order_by(
                ExchangeAssetModel.mic, ExchangeAssetModel.symbol, "sid")
            results: list[ExchangeAssetModel] = list((await session.execute(q)).scalars())

            q_equities = select(EquityModel).where(
                EquityModel.id.in_([equity_exchange.asset_id for equity_exchange in results])).options(
                selectinload(EquityModel.asset_router))
            assets: list[EquityModel] = list((await session.execute(q_equities)).scalars())
            assets_by_id = {asset.id: asset for asset in assets}
            all_assets = await self.get_all_assets()
        return [ExchangeAsset(
            sid=asset.sid,
            start_date=asset.start_date,
            first_traded=asset.first_traded,
            end_date=asset.end_date,
            auto_close_date=asset.auto_close_date,
            symbol=asset.symbol,
            exchange=await self.get_exchange_by_mic(mic=asset.mic),
            asset=Equity(
                first_traded=assets_by_id[asset.asset_id].first_traded,
                auto_close_date=assets_by_id[asset.asset_id].auto_close_date,
                end_date=assets_by_id[asset.asset_id].end_date,
                start_date=assets_by_id[asset.asset_id].start_date,
                isin=assets_by_id[asset.asset_id].isin,
                asset_name=assets_by_id[asset.asset_id].asset_name,
                id=assets_by_id[asset.asset_id].id,
            ),
            quote=all_assets[asset.quote_id],
            external_id=asset.external_id,
        ) for asset in results]

    async def get_equities_by_isins(self, isins: list[str]) -> list[Equity]:
        async with self.session_maker() as session:
            q_equities = select(
                EquityModel
            ).where(
                EquityModel.isin.in_(isins)
            ).options(
                selectinload(EquityModel.asset_router)
            )
            assets: list[EquityModel] = list((await session.execute(q_equities)).scalars())

            return [Equity(
                id=asset.id,
                asset_name=asset.asset_name,
                start_date=asset.start_date,
                first_traded=asset.first_traded,
                end_date=asset.end_date,
                auto_close_date=asset.auto_close_date,
                isin=asset.isin
            ) for asset in assets]

    async def get_equity_by_symbol(self, symbol: str, exchange_name: str) -> Equity | None:
        if exchange_name is None:
            equities = await self.get_equities_by_symbols(symbols=[symbol])
        else:
            equities = await self.get_equities_by_symbols_and_exchange(symbols=[symbol], exchange_name=exchange_name)
        if equities:
            return equities[0]
        return None

    async def get_exchange_equity_by_symbol(self, symbol: AssetSymbol) -> ExchangeAsset | None:
        equities = await self.get_exchange_equities_by_symbols(symbols=[symbol])
        if equities:
            return equities[0]
        return None

    async def get_exchange_currency_by_symbol(self, symbol: AssetSymbol) -> ExchangeAsset | None:
        currencies = await self.get_exchange_currencies_by_symbols(symbols=[symbol])
        if currencies:
            return currencies[0]
        return None

    async def get_cash_dividends_with_ex_date(self, assets: list[Asset], date: datetime.date) -> list[DividendPayout]:
        async with self.session_maker() as session:
            all_assets = await self.get_all_assets()
            q_dividends = select(
                DividendPayoutModel
            ).where(
                DividendPayoutModel.asset_id.in_([asset.id for asset in assets]),
                DividendPayoutModel.ex_date == date
            )
            dividends_r: list[DividendPayoutModel] = list((await session.execute(q_dividends)).scalars())

        return [
            DividendPayout(
                asset=all_assets[d.asset_id],
                amount=d.amount,
                pay_date=d.pay_date,
                declared_date=d.declared_date,
                record_date=d.record_date,
                ex_date=d.ex_date,
                currency=None
                # currency
            )
            for d in dividends_r]

    async def get_splits(self, assets: list[Asset], date: datetime.date) -> list[Split]:
        async with self.session_maker() as session:
            all_assets = await self.get_all_assets()
            q_splits = select(
                SplitModel
            ).where(
                SplitModel.asset_id.in_([asset.id for asset in assets]),
                SplitModel.effective_date == date
            )
            splits_r: list[SplitModel] = list((await session.execute(q_splits)).scalars())

        return [
            Split(
                asset=all_assets[s.asset_id],
                effective_date=s.effective_date,
                ratio=s.ratio,
                id=s.id
            )
            for s in splits_r]

    def migrate(self) -> None:
        alembic_dir_path = Path(pathlib.Path(__file__).parent.parent.parent, "alembic")
        alembic_cfg = config.Config(Path(alembic_dir_path, "alembic.ini"))
        alembic_cfg.set_main_option("script_location", str(Path(alembic_dir_path)))
        alembic_cfg.set_main_option("sqlalchemy.url", self.db_url.replace("+aiosqlite", ""))
        # os.makedirs(db_path.parent, exist_ok=True)
        # Run the migration
        command.upgrade(alembic_cfg, "head")

    @property
    def exchange_info(self):
        with self.engine.connect() as conn:
            es = conn.execute(sa.select(self.exchanges.c)).fetchall()
        return {
            name: ExchangeInfoModel(name, canonical_name, country_code)
            for name, canonical_name, country_code in es
        }

    @property
    def symbol_ownership_map(self):
        out = {}
        for mappings in self.symbol_ownership_maps_by_country_code.values():
            for key, ownership_periods in mappings.items():
                out.setdefault(key, []).extend(ownership_periods)

        return out

    @property
    def symbol_ownership_maps_by_country_code(self):
        with self.engine.connect() as conn:
            query = sa.select(
                self.equities.c.sid,
                self.exchanges.c.country_code,
            ).where(self.equities.c.exchange == self.exchanges.c.exchange)
            sid_to_country_code = dict(conn.execute(query).fetchall())

            return build_grouped_ownership_map(
                conn,
                table=self.equity_symbol_mappings,
                key_from_row=(lambda row: (row.company_symbol, row.share_class_symbol)),
                value_from_row=lambda row: row.symbol,
                group_key=lambda row: sid_to_country_code[row.sid],
            )

    def lookup_asset_types(self, sids: list[int]):
        """Retrieve asset types for a list of sids.

        Parameters
        ----------
        sids : list[int]

        Returns
        -------
        types : dict[sid -> str or None]
            Asset types for the provided sids.
        """
        found = {}
        missing = set()

        for sid in sids:
            try:
                found[sid] = self._asset_type_cache[sid]
            except KeyError:
                missing.add(sid)

        if not missing:
            return found

        router_cols = self.asset_router.c

        with self.engine.connect() as conn:
            for assets in group_into_chunks(missing):
                query = sa.select(router_cols.sid, router_cols.asset_type).where(
                    self.asset_router.c.sid.in_(map(int, assets))
                )
                for sid, type_ in conn.execute(query).fetchall():
                    missing.remove(sid)
                    found[sid] = self._asset_type_cache[sid] = type_

                for sid in missing:
                    found[sid] = self._asset_type_cache[sid] = None

        return found

    def group_by_type(self, sids: list[int]):
        """Group a list of sids by asset type.

        Parameters
        ----------
        sids : list[int]

        Returns
        -------
        types : dict[str or None -> list[int]]
            A dict mapping unique asset types to lists of sids drawn from sids.
            If we fail to look up an asset, we assign it a key of None.
        """
        return invert(self.lookup_asset_types(sids))

    def retrieve_asset(self, sid: int, default_none: bool = False):
        """
        Retrieve the Asset for a given sid.
        """
        try:
            asset = self._asset_cache[sid]
            if asset is None and not default_none:
                raise SidsNotFound(sids=[sid])
            return asset
        except KeyError:
            return self.retrieve_all(sids=[sid, ], default_none=default_none)[0]

    async def retrieve_all(self, sids: list[int], default_none: bool = False):
        """Retrieve all assets in `sids`.

        Parameters
        ----------
        sids : iterable of int
            Assets to retrieve.
        default_none : bool
            If True, return None for failed lookups.
            If False, raise `SidsNotFound`.

        Returns
        -------
        assets : list[AssetModel or None]
            A list of the same length as `sids` containing Assets (or Nones)
            corresponding to the requested sids.

        Raises
        ------
        SidsNotFound
            When a requested sid is not found and default_none=False.
        """

        async with self.session_maker() as session:
            q = select(AssetRouter).where(AssetModel.sid.in_(sids))
            assets = (await session.execute(q)).scalars()
            return list(assets)

        hits, missing, failures = {}, set(), []
        for sid in sids:
            try:
                asset = self._asset_cache[sid]
                if not default_none and asset is None:
                    # Bail early if we've already cached that we don't know
                    # about an asset.
                    raise SidsNotFound(sids=[sid])
                hits[sid] = asset
            except KeyError:
                missing.add(sid)

        # All requests were cache hits.  Return requested sids in order.
        if not missing:
            return [hits[sid] for sid in sids]

        update_hits = hits.update

        # Look up cache misses by type.
        type_to_assets = self.group_by_type(sids=missing)

        # Handle failures
        failures = {failure: None for failure in type_to_assets.pop(None, ())}
        update_hits(failures)
        self._asset_cache.update(failures)

        if failures and not default_none:
            raise SidsNotFound(sids=list(failures))

        # We don't update the asset cache here because it should already be
        # updated by `self.retrieve_equities`.
        update_hits(self.retrieve_equities(sids=type_to_assets.pop("equity", [])))
        update_hits(self.retrieve_futures_contracts(sids=type_to_assets.pop("future", [])))

        # We shouldn't know about any other asset types.
        if type_to_assets:
            raise AssertionError("Found asset types: %s" % list(type_to_assets.keys()))

        return [hits[sid] for sid in sids]

    def retrieve_equities(self, sids: list[int]):
        """Retrieve Equity objects for a list of sids.

        Users generally shouldn't need to this method (instead, they should
        prefer the more general/friendly `retrieve_assets`), but it has a
        documented interface and tests because it's used upstream.

        Parameters
        ----------
        sids : iterable[int]

        Returns
        -------
        equities : dict[int -> Equity]

        Raises
        ------
        EquitiesNotFound
            When any requested asset isn't found.
        """
        return self._retrieve_assets(sids=sids, asset_tbl=self.equities, asset_type=Equity)

    # def _retrieve_equity(self, sid):
    #     return self.retrieve_equities(sids=[sid, ])[sid]

    def retrieve_futures_contracts(self, sids: list[int]):
        """Retrieve Future objects for an iterable of sids.

        Users generally shouldn't need to this method (instead, they should
        prefer the more general/friendly `retrieve_assets`), but it has a
        documented interface and tests because it's used upstream.

        Parameters
        ----------
        sids : iterable[int]

        Returns
        -------
        equities : dict[int -> Equity]

        Raises
        ------
        EquitiesNotFound
            When any requested asset isn't found.
        """
        return self._retrieve_assets(sids=sids, asset_tbl=self.futures_contracts, asset_type=FuturesContract)

    @staticmethod
    def _select_assets_by_sid(asset_tbl: Table, sids: list[int]):
        return sa.select(asset_tbl).where(asset_tbl.c.sid.in_(map(int, sids)))

    @staticmethod
    def _select_asset_by_symbol(asset_tbl: Table, symbol: str):
        return sa.select(asset_tbl).where(asset_tbl.c.symbol == symbol)

    def _select_most_recent_symbols_chunk(self, sid_group: list[int]):
        """Retrieve the most recent symbol for a set of sids.

        Parameters
        ----------
        sid_group : iterable[int]
            The sids to lookup. The length of this sequence must be less than
            or equal to SQLITE_MAX_VARIABLE_NUMBER because the sids will be
            passed in as sql bind params.

        Returns
        -------
        sel : Selectable
            The sqlalchemy selectable that will query for the most recent
            symbol for each sid.

        Notes
        -----
        This is implemented as an inner select of the columns of interest
        ordered by the end date of the (sid, symbol) mapping. We then group
        that inner select on the sid with no aggregations to select the last
        row per group which gives us the most recently active symbol for all
        of the sids.
        """
        cols = self.equity_symbol_mappings.c

        # These are the columns we actually want.
        data_cols = (cols.sid,) + tuple(cols[name] for name in SYMBOL_COLUMNS)

        # Also select the max of end_date so that all non-grouped fields take
        # on the value associated with the max end_date.
        # to_select = data_cols + (sa.func.max(cols.end_date),)
        func_rank = (
            sa.func.rank()
            .over(order_by=cols.end_date.desc(), partition_by=cols.sid)
            .label("rnk")
        )
        to_select = data_cols + (func_rank,)

        subquery = (
            sa.select(*to_select)
            .where(cols.sid.in_(map(int, sid_group)))
            .subquery("sq")
        )
        query = (
            sa.select(subquery.columns)
            .filter(subquery.c.rnk == 1)
            .select_from(subquery)
        )
        return query

    def _lookup_most_recent_symbols(self, sids: list[int]):
        with self.engine.connect() as conn:
            return {
                row.sid: {c: row[c] for c in SYMBOL_COLUMNS}
                for row in concat(
                    conn.execute(self._select_most_recent_symbols_chunk(sid_group=sid_group))
                    .mappings()
                    .fetchall()
                    for sid_group in partition_all(n=SQLITE_MAX_VARIABLE_NUMBER, seq=sids)
                )
            }

    def _retrieve_asset_dicts(self, sids: list[int], asset_tbl: Table, querying_equities):
        if not sids:
            return

        if querying_equities:

            def mkdict(
                    row,
                    exchanges=self.exchange_info,
                    symbols=self._lookup_most_recent_symbols(sids=sids),
            ):
                d = dict(row)
                d["exchange_info"] = exchanges[d.pop("exchange")]
                # we are not required to have a symbol for every asset, if
                # we don't have any symbols we will just use the empty string
                return merge(d, symbols.get(row["sid"], {}))

        else:

            def mkdict(row, exchanges=self.exchange_info):
                d = dict(row)
                d["exchange_info"] = exchanges[d.pop("exchange")]
                return d

        for assets in group_into_chunks(sids):
            # Load misses from the db.
            query = self._select_assets_by_sid(asset_tbl, assets)

            with self.engine.connect() as conn:
                for row in conn.execute(query).mappings().fetchall():
                    yield _convert_asset_timestamp_fields(mkdict(row))

    def _retrieve_assets(self, sids: list[int], asset_tbl: Table, asset_type: type):
        """Internal function for loading assets from a table.

        This should be the only method of `AssetFinder` that writes Assets into
        self._asset_cache.

        Parameters
        ---------
        sids : iterable of int
            Asset ids to look up.
        asset_tbl : sqlalchemy.Table
            Table from which to query assets.
        asset_type : type
            Type of asset to be constructed.

        Returns
        -------
        assets : dict[int -> Asset]
            Dict mapping requested sids to the retrieved assets.
        """
        # Fastpath for empty request.
        if not sids:
            return {}

        cache = self._asset_cache
        hits = {}

        querying_equities = issubclass(asset_type, Equity)
        filter_kwargs = (
            _filter_equity_kwargs if querying_equities else _filter_future_kwargs
        )

        rows = self._retrieve_asset_dicts(sids, asset_tbl, querying_equities)
        for row in rows:
            sid = row["sid"]
            asset = asset_type(**filter_kwargs(row))
            hits[sid] = cache[sid] = asset

        # If we get here, it means something in our code thought that a
        # particular sid was an equity/future and called this function with a
        # concrete type, but we couldn't actually resolve the asset.  This is
        # an error in our code, not a user-input error.
        misses = tuple(set(sids) - hits.keys())
        if misses:
            if querying_equities:
                raise EquitiesNotFound(sids=misses)
            else:
                raise FutureContractsNotFound(sids=misses)
        return hits

    def _lookup_symbol_strict(self, ownership_map: dict[(str, str), list[OwnershipPeriod]], multi_country: bool,
                              symbol: str, as_of_date: datetime.datetime):
        """Resolve a symbol to an asset object without fuzzy matching.

        Parameters
        ----------
        ownership_map : dict[(str, str), list[OwnershipPeriod]]
            The mapping from split symbols to ownership periods.
        multi_country : bool
            Does this mapping span multiple countries?
        symbol : str
            The symbol to look up.
        as_of_date : datetime or None
            If multiple assets have held this sid, which day should the
            resolution be checked against? If this value is None and multiple
            sids have held the ticker, then a MultipleSymbolsFound error will
            be raised.

        Returns
        -------
        asset : AssetModel
            The asset that held the given symbol.

        Raises
        ------
        SymbolNotFound
            Raised when the symbol or symbol as_of_date pair do not map to
            any assets.
        MultipleSymbolsFound
            Raised when multiple assets held the symbol. This happens if
            multiple assets held the symbol at disjoint times and
            ``as_of_date`` is None, or if multiple assets held the symbol at
            the same time and``multi_country`` is True.

        Notes
        -----
        The resolution algorithm is as follows:

        - Split the symbol into the company and share class component.
        - Do a dictionary lookup of the
          ``(company_symbol, share_class_symbol)`` in the provided ownership
          map.
        - If there is no entry in the dictionary, we don't know about this
          symbol so raise a ``SymbolNotFound`` error.
        - If ``as_of_date`` is None:
          - If more there is more than one owner, raise
            ``MultipleSymbolsFound``
          - Otherwise, because the list mapped to a symbol cannot be empty,
            return the single asset.
        - Iterate through all of the owners:
          - If the ``as_of_date`` is between the start and end of the ownership
            period:
            - If multi_country is False, return the found asset.
            - Otherwise, put the asset in a list.
        - At the end of the loop, if there are no candidate assets, raise a
          ``SymbolNotFound``.
        - If there is exactly one candidate, return it.
        - Othewise, raise ``MultipleSymbolsFound`` because the ticker is not
          unique across countries.
        """
        # split the symbol into the components, if there are no
        # company/share class parts then share_class_symbol will be empty
        company_symbol, share_class_symbol = split_delimited_symbol(symbol=symbol)
        try:
            owners = ownership_map[company_symbol, share_class_symbol]
            assert owners, "empty owners list for %r" % symbol
        except KeyError as exc:
            # no equity has ever held this symbol
            raise SymbolNotFound(symbol=symbol) from exc

        if not as_of_date:
            # exactly one equity has ever held this symbol, we may resolve
            # without the date
            if len(owners) == 1:
                return self.retrieve_asset(sid=owners[0].sid)

            options = {self.retrieve_asset(sid=owner.sid) for owner in owners}

            if multi_country:
                country_codes = map(attrgetter("country_code"), options)

                if len(set(country_codes)) > 1:
                    raise SameSymbolUsedAcrossCountries(
                        symbol=symbol, options=dict(zip(country_codes, options))
                    )

            # more than one equity has held this ticker, this
            # is ambiguous without the date
            raise MultipleSymbolsFound(symbol=symbol, options=options)

        options = []
        country_codes = []
        for start, end, sid, _ in owners:
            if start.date() <= as_of_date < end.date():
                # find the equity that owned it on the given asof date
                asset = self.retrieve_asset(sid=sid)

                # if this asset owned the symbol on this asof date and we are
                # only searching one country, return that asset
                if not multi_country:
                    return asset
                else:
                    options.append(asset)
                    country_codes.append(asset.country_code)

        if not options:
            # no equity held the ticker on the given asof date
            raise SymbolNotFound(symbol=symbol)

        # if there is one valid option given the asof date, return that option
        if len(options) == 1:
            return options[0]

        # if there's more than one option given the asof date, a country code
        # must be passed to resolve the symbol to an asset
        raise SameSymbolUsedAcrossCountries(
            symbol=symbol, options=dict(zip(country_codes, options))
        )

    def _choose_symbol_ownership_map(self, country_code: str):
        if country_code is None:
            return self.symbol_ownership_map

        return self.symbol_ownership_maps_by_country_code.get(country_code)

    def lookup_symbol(self, symbol: str, as_of_date: datetime.datetime,
                      country_code: str | None = None):
        """Lookup an equity by symbol.

        Parameters
        ----------
        symbol : str
            The ticker symbol to resolve.
        as_of_date : datetime.datetime or None
            Look up the last owner of this symbol as of this datetime.
            If ``as_of_date`` is None, then this can only resolve the equity
            if exactly one equity has ever owned the ticker.
        country_code : str or None, optional
            The country to limit searches to. If not provided, the search will
            span all countries which increases the likelihood of an ambiguous
            lookup.

        Returns
        -------
        equity : Equity
            The equity that held ``symbol`` on the given ``as_of_date``, or the
            only equity to hold ``symbol`` if ``as_of_date`` is None.

        Raises
        ------
        SymbolNotFound
            Raised when no equity has ever held the given symbol.
        MultipleSymbolsFound
            Raised when no ``as_of_date`` is given and more than one equity
            has held ``symbol``. This is also raised when ``fuzzy=True`` and
            there are multiple candidates for the given ``symbol`` on the
            ``as_of_date``. Also raised when no ``country_code`` is given and
            the symbol is ambiguous across multiple countries.
        """
        if symbol is None:
            raise TypeError(
                "Cannot lookup asset for symbol of None for "
                "as of date %s." % as_of_date
            )

        f = self._lookup_symbol_strict
        mapping = self._choose_symbol_ownership_map(country_code)

        if mapping is None:
            raise SymbolNotFound(symbol=symbol)
        return f(
            mapping,
            country_code is None,
            symbol,
            as_of_date,
        )

    def lookup_future_symbol(self, symbol: str):
        """Lookup a future contract by symbol.

        Parameters
        ----------
        symbol : str
            The symbol of the desired contract.

        Returns
        -------
        future : Future
            The future contract referenced by ``symbol``.

        Raises
        ------
        SymbolNotFound
            Raised when no contract named 'symbol' is found.

        """
        with self.engine.connect() as conn:
            data = (
                conn.execute(
                    self._select_asset_by_symbol(asset_tbl=self.futures_contracts, symbol=symbol)
                )
                .mappings()
                .fetchone()
            )

        # If no data found, raise an exception
        if not data:
            raise SymbolNotFound(symbol=symbol)
        return self.retrieve_asset(sid=data["sid"])

    def _get_contract_sids(self, root_symbol: str):
        fc_cols = self.futures_contracts.c
        with self.engine.connect() as conn:
            return (
                conn.execute(
                    sa.select(
                        fc_cols.sid,
                    )
                    .where(
                        (fc_cols.root_symbol == root_symbol)
                        & (fc_cols.start_date != pd.NaT.value)
                    )
                    .order_by(fc_cols.sid)
                )
                .scalars()
                .fetchall()
            )

    def _get_root_symbol_exchange(self, root_symbol: str):
        fc_cols = self.futures_root_symbols.c
        fields = (fc_cols.exchange,)

        with self.engine.connect() as conn:
            exchange = conn.execute(
                sa.select(*fields).where(fc_cols.root_symbol == root_symbol)
            ).scalar()

        if exchange is not None:
            return exchange
        else:
            raise SymbolNotFound(symbol=root_symbol)

    def get_ordered_contracts(self, root_symbol: str):
        try:
            return self._ordered_contracts[root_symbol]
        except KeyError:
            contract_sids = self._get_contract_sids(root_symbol)
            contracts = deque(self.retrieve_all(contract_sids))
            chain_predicate = self._future_chain_predicates.get(root_symbol, None)
            oc = OrderedContracts(root_symbol, contracts, chain_predicate)
            self._ordered_contracts[root_symbol] = oc
            return oc

    def create_continuous_future(self, root_symbol: str, offset: int, roll_style: str, adjustment: str):
        if adjustment not in ADJUSTMENT_STYLES:
            raise ValueError(
                f"Invalid adjustment style {adjustment!r}. Allowed adjustment styles are "
                f"{list(ADJUSTMENT_STYLES)}."
            )

        oc = self.get_ordered_contracts(root_symbol=root_symbol)
        exchange = self._get_root_symbol_exchange(root_symbol=root_symbol)

        sid = _encode_continuous_future_sid(root_symbol=root_symbol, offset=offset, roll_style=roll_style,
                                            adjustment_style=None)
        mul_sid = _encode_continuous_future_sid(root_symbol=root_symbol, offset=offset, roll_style=roll_style,
                                                adjustment_style="div")
        add_sid = _encode_continuous_future_sid(root_symbol=root_symbol, offset=offset, roll_style=roll_style,
                                                adjustment_style="add")

        cf_template = partial(
            ContinuousFuture,
            root_symbol=root_symbol,
            offset=offset,
            roll_style=roll_style,
            start_date=oc.start_date,
            end_date=oc.end_date,
            exchange_info=self.exchange_info[exchange],
        )

        cf = cf_template(sid=sid)
        mul_cf = cf_template(sid=mul_sid, adjustment="mul")
        add_cf = cf_template(sid=add_sid, adjustment="add")

        self._asset_cache[cf.sid] = cf
        self._asset_cache[mul_cf.sid] = mul_cf
        self._asset_cache[add_cf.sid] = add_cf

        return {None: cf, "mul": mul_cf, "add": add_cf}[adjustment]

    @aiocache.cached(cache=Cache.MEMORY)
    async def _compute_lifetimes(self, country_codes: frozenset[str]) -> Lifetimes:
        """Compute and cache a recarray of asset lifetimes"""
        sids = starts = ends = []
        async with self.session_maker() as session:
            sids_subquery = select(EquitySymbolMappingModel.sid).join(
                ExchangeInfoModel, onclause=ExchangeInfoModel.exchange == EquitySymbolMappingModel.exchange
            ).where(ExchangeInfoModel.country_code.in_(country_codes))
            q = select(
                EquityModel.sid,
                EquityModel.start_date,
                EquityModel.end_date
            ).where(EquityModel.sid.in_(sids_subquery))
            result = list((await session.execute(q)))
            if result:
                sids, starts, ends = zip(*result)

        sid = np.array(sids, dtype="i8")
        start = np.array(
            [datetime.datetime.combine(s, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc).timestamp() for s
             in starts], dtype="f8")
        end = np.array(
            [datetime.datetime.combine(s, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc).timestamp() for s
             in ends], dtype="f8")
        start[np.isnan(start)] = 0  # convert missing starts to 0
        end[np.isnan(end)] = np.iinfo(int).max  # convert missing end to INTMAX
        return Lifetimes(sid, start.astype("i8"), end.astype("i8"))

    async def lifetimes(self, dates: pd.DatetimeIndex, include_start_date: bool, country_codes: list[str]):
        """Compute a DataFrame representing asset lifetimes for the specified date
        range.

        Parameters
        ----------
        dates : pd.DatetimeIndex
            The dates for which to compute lifetimes.
        include_start_date : bool
            Whether or not to count the asset as alive on its start_date.

            This is useful in a backtesting context where `lifetimes` is being
            used to signify "do I have data for this asset as of the morning of
            this date?"  For many financial metrics, (e.g. daily close), data
            isn't available for an asset until the end of the asset's first
            day.
        country_codes : iterable[str]
            The country codes to get lifetimes for.

        Returns
        -------
        lifetimes : pd.DataFrame
            A frame of dtype bool with `dates` as index and an Int64Index of
            assets as columns.  The value at `lifetimes.loc[date, asset]` will
            be True iff `asset` existed on `date`.  If `include_start_date` is
            False, then lifetimes.loc[date, asset] will be false when date ==
            asset.start_date.

        See Also
        --------
        numpy.putmask
        ziplime.pipeline.engine.SimplePipelineEngine._compute_root_mask
        """
        lifetimes = await self._compute_lifetimes(country_codes=frozenset(country_codes))
        return lifetimes

    @aiocache.cached(cache=Cache.MEMORY)
    async def _compute_asset_lifetimes(self, assets: frozenset[Asset]) -> Lifetimes:
        """Compute and cache a recarray of asset lifetimes"""
        # sids = starts = ends = []
        # async with self.session_maker() as session:
        #     sids_subquery = select(EquitySymbolMappingModel.sid).join(
        #         ExchangeInfo, onclause=ExchangeInfo.exchange == EquitySymbolMappingModel.exchange
        #     ).where(ExchangeInfo.country_code.in_(country_codes))
        #     q = select(
        #         EquityModel.sid,
        #         EquityModel.start_date,
        #         EquityModel.end_date
        #     ).where(EquityModel.sid.in_(sids_subquery))
        #     result = list((await session.execute(q)))
        #     if result:
        #         sids, starts, ends = zip(*result)
        sids = [asset.sid for asset in assets]
        starts = [asset.start_date for asset in assets]
        ends = [asset.end_date for asset in assets]

        sid = np.array(sids, dtype="i8")
        start = np.array(
            [datetime.datetime.combine(s, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc).timestamp() for s
             in starts], dtype="f8")
        end = np.array(
            [datetime.datetime.combine(s, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc).timestamp() for s
             in ends], dtype="f8")
        start[np.isnan(start)] = 0  # convert missing starts to 0
        end[np.isnan(end)] = np.iinfo(int).max  # convert missing end to INTMAX
        return Lifetimes(sid, start.astype("i8"), end.astype("i8"))

    async def asset_lifetimes(self, assets: list[Asset], dates: pd.DatetimeIndex, include_start_date: bool):
        """Compute a DataFrame representing asset lifetimes for the specified date
        range.

        Parameters
        ----------
        dates : pd.DatetimeIndex
            The dates for which to compute lifetimes.
        include_start_date : bool
            Whether or not to count the asset as alive on its start_date.

            This is useful in a backtesting context where `lifetimes` is being
            used to signify "do I have data for this asset as of the morning of
            this date?"  For many financial metrics, (e.g. daily close), data
            isn't available for an asset until the end of the asset's first
            day.
        country_codes : iterable[str]
            The country codes to get lifetimes for.

        Returns
        -------
        lifetimes : pd.DataFrame
            A frame of dtype bool with `dates` as index and an Int64Index of
            assets as columns.  The value at `lifetimes.loc[date, asset]` will
            be True iff `asset` existed on `date`.  If `include_start_date` is
            False, then lifetimes.loc[date, asset] will be false when date ==
            asset.start_date.

        See Also
        --------
        numpy.putmask
        ziplime.pipeline.engine.SimplePipelineEngine._compute_root_mask
        """
        lifetimes = await self._compute_asset_lifetimes(assets=frozenset(assets))
        return lifetimes

    # def equities_sids_for_country_code(self, country_code: str):
    #     """Return all of the sids for a given country.
    #
    #     Parameters
    #     ----------
    #     country_code : str
    #         An ISO 3166 alpha-2 country code.
    #
    #     Returns
    #     -------
    #     tuple[int]
    #         The sids whose exchanges are in this country.
    #     """
    #     sids = self._compute_asset_lifetimes(country_codes=[country_code]).sid
    #     return tuple(sids.tolist())

    # def equities_sids_for_exchange_name(self, exchange_name: str):
    #     """Return all of the sids for a given exchange_name.
    #
    #     Parameters
    #     ----------
    #     exchange_name : str
    #
    #     Returns
    #     -------
    #     tuple[int]
    #         The sids whose exchanges are in this country.
    #     """
    #     sids = self._compute_asset_lifetimes(exchange_names=[exchange_name]).sid
    #     return tuple(sids.tolist())

    def to_json(self):
        return {
            "db_url": self.db_url
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(
            db_url=data["db_url"],
            future_chain_predicates=CHAIN_PREDICATES
        )
