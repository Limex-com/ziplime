import dataclasses
import datetime
from collections.abc import Iterable
from logging import Logger

import aiocache
import pandas as pd
import polars as pl
import structlog
from aiocache import Cache

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.futures_root import FuturesRoot
from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.domain.ordered_contracts import OrderedContracts
from ziplime.assets.entities.split import Split
from ziplime.assets.entities.stock_dividend_payout import StockDividendPayout
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.repositories.adjustments_repository import AdjustmentRepository
from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.errors import AmbiguousSymbol
from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.assets.entities.asset import Asset

class AssetService:

    def __init__(self, asset_repository: AssetRepository, adjustments_repository: AdjustmentRepository,
                 logger: Logger = structlog.get_logger(__name__)):
        self._asset_repository = asset_repository
        self._adjustments_repository = adjustments_repository
        self._logger = logger

    async def save_equities(self, equities: list[Equity]) -> list[Equity]:
        return await self._asset_repository.save_equities(equities=equities)

    async def save_bonds(self, bonds: list[Bond]) -> list[Bond]:
        return await self._asset_repository.save_bonds(bonds=bonds)

    async def save_bond_events(self, bond_events: list[BondEvent]) -> list[BondEvent]:
        return await self._asset_repository.save_bond_events(bond_events=bond_events)

    async def get_bond_events(self, bonds: list[Bond]) -> dict[int, list[BondEvent]]:
        """Return the coupon, amortization, maturity and offer schedule of each bond."""
        return await self._asset_repository.get_bond_events(bonds=bonds)

    async def get_exchange_bonds_by_symbols(self, symbols: list[AssetSymbol]) -> list[ExchangeAsset]:
        return await self._asset_repository.get_exchange_bonds_by_symbols(symbols=symbols)

    async def get_exchange_bond_by_symbol(self, symbol: AssetSymbol) -> ExchangeAsset | None:
        return await self._asset_repository.get_exchange_bond_by_symbol(symbol=symbol)

    async def get_all_bond_listings(self, mic: str | None = None) -> list[ExchangeAsset]:
        """Every stored bond listing, ordered by maturity."""
        return await self._asset_repository.get_all_bond_listings(mic=mic)

    async def get_bonds_by_isins(self, isins: list[str]) -> list[Bond]:
        return await self._asset_repository.get_bonds_by_isins(isins=isins)

    async def save_symbol_universe(self, symbol_universe: SymbolsUniverse) -> None:
        await self._asset_repository.save_symbol_universe(symbol_universe=symbol_universe)

    async def save_futures_roots(self, futures_roots: list[FuturesRoot]) -> list[FuturesRoot]:
        return await self._asset_repository.save_futures_roots(futures_roots=futures_roots)

    async def save_futures_contracts(self, futures_contracts: list[FuturesContract]) -> list[FuturesContract]:
        return await self._asset_repository.save_futures_contracts(futures_contracts=futures_contracts)

    async def get_futures_roots(self) -> dict[str, FuturesRoot]:
        return await self._asset_repository.get_futures_roots()

    async def get_exchange_futures_contracts_by_root(self, root_symbol: str,
                                                     mic: str | None = None) -> list[ExchangeAsset]:
        """Return the contract chain of ``root_symbol``, ordered by expiration."""
        return await self._asset_repository.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol, mic=mic)

    async def get_ordered_contracts(self, root_symbol: str, mic: str | None = None) -> OrderedContracts:
        """Return the contract chain of ``root_symbol``, ordered by expiration."""
        return await self._asset_repository.get_ordered_contracts(root_symbol=root_symbol, mic=mic)

    async def create_continuous_future(self, root_symbol: str, offset: int = 0,
                                       roll_style: str = "volume",
                                       adjustment: str | None = "mul") -> ContinuousFuture:
        """Build a continuous future specifier over a stored chain."""
        return await self._asset_repository.create_continuous_future(
            root_symbol=root_symbol, offset=offset, roll_style=roll_style, adjustment=adjustment)

    async def save_commodities(self, commodities: list[Commodity]) -> list[Commodity]:
        await self._asset_repository.save_commodities(commodities=commodities)

    async def save_currencies(self, currencies: list[Currency]) -> list[Currency]:
        return await self._asset_repository.save_currencies(currencies=currencies)

    async def import_assets(self, assets_import: AssetsImport) -> None:
        """Persist a batch of assets, respecting the references between them.

        Currencies, commodities and equities are written first because a futures contract points at
        its underlying and its chain; listings go last because they reference an already stored
        asset and mint the ``sid`` that data bundles key bars by.
        """
        await self.save_currencies(currencies=assets_import.currencies)
        await self.save_commodities(commodities=assets_import.commodities)
        await self.save_equities(equities=assets_import.equities)
        bonds = await self.save_bonds(bonds=assets_import.bonds)
        await self.save_futures_roots(futures_roots=assets_import.futures_roots)
        await self.save_futures_contracts(futures_contracts=assets_import.futures)
        await self.save_exchange_assets(exchange_assets=assets_import.exchange_assets)
        # Schedules are attached after the bonds exist, and to the *stored* bonds: the entities the
        # caller built still carry ``id=None``, which no event row could point at.
        if assets_import.bond_events:
            await self.save_bond_events(
                bond_events=_rebind_bond_events(assets_import.bond_events, bonds))
        self._logger.info(
            "Imported assets",
            currencies=len(assets_import.currencies),
            commodities=len(assets_import.commodities),
            equities=len(assets_import.equities),
            bonds=len(assets_import.bonds),
            bond_events=len(assets_import.bond_events),
            futures_roots=len(assets_import.futures_roots),
            futures=len(assets_import.futures),
            exchange_assets=len(assets_import.exchange_assets),
        )

    async def import_bond_events(self, bond_events: list[BondEvent]) -> list[BondEvent]:
        return await self._asset_repository.save_bond_events(bond_events=bond_events)

    async def import_dividends(self, dividends: list[DividendPayout]):
        return await self._asset_repository.save_dividends(dividends=dividends)

    async def import_splits(self, splits: list[Split]):
        return await self._asset_repository.save_splits(splits=splits)

    async def save_exchange_assets(self, exchange_assets: list[ExchangeAsset]) -> list[ExchangeAsset]:
        return await self._asset_repository.save_exchange_assets(exchange_assets=exchange_assets)

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

    async def get_exchange_assets_by_symbols(self, symbols: list[AssetSymbol],
                                             asset_type: AssetType | Iterable[AssetType]) -> list[
        ExchangeAsset | None]:
        """Resolve ``(symbol, mic)`` pairs to listings, trying each candidate asset type in turn.

        Accepting several types is what lets one data bundle hold more than one asset class. A
        cross-asset strategy -- an equity hedged with a futures contract, a bond portfolio with an
        equity sleeve -- needs its instruments in a single bundle, and a bundle keyed to one type
        rejects every symbol of the others.

        A ticker is only unique **within** an asset class, so a symbol that resolves under more
        than one of the candidates raises :class:`~ziplime.errors.AmbiguousSymbol` rather than
        picking one. Taking the first match would look like it worked and quietly tag the data
        with another instrument's sid -- the same ticker can be a futures contract on one venue
        and, because an equity vendor listed it that way, an equity on another.

        Raises:
            AmbiguousSymbol: if a symbol resolves under more than one of ``asset_type``.
        """
        asset_types = ([asset_type] if isinstance(asset_type, AssetType)
                       else list(asset_type))
        resolved = []
        for symbol in symbols:
            matches = {}
            for candidate in asset_types:
                found = await self.get_exchange_asset_by_symbol(symbol=symbol,
                                                                asset_type=candidate)
                if found is not None:
                    matches[candidate] = found
            if len(matches) > 1:
                raise AmbiguousSymbol(
                    symbol=f"{symbol.symbol}@{symbol.mic}" if symbol.mic else symbol.symbol,
                    asset_types=", ".join(t.value for t in matches))
            resolved.append(next(iter(matches.values()), None))
        return resolved

    async def get_futures_contract_by_symbol(self, symbol: str, mic: str | None = None) -> FuturesContract | None:
        return await self._asset_repository.get_futures_contract_by_symbol(symbol=symbol, mic=mic)

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_currency_by_symbol(self, symbol: str) -> Currency | None:
        currency = await self._asset_repository.get_currency_by_symbol(
            symbol=symbol
        )
        return currency

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_exchange_currency_by_symbol(self, symbol: AssetSymbol) -> ExchangeAsset | None:
        return await self._asset_repository.get_exchange_asset_by_symbol(symbol=symbol, asset_type=AssetType.CURRENCY)

    async def get_commodity_by_symbol(self, symbol: str, mic: str) -> Commodity | None:
        return await self._asset_repository.get_commodity_by_symbol(symbol=symbol,
                                                                    mic=mic)

    async def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[StockDividendPayout]:
        return await self._adjustments_repository.get_stock_dividends(sid=sid,
                                                                      trading_days=trading_days)

    async def get_splits(self, assets: list[Asset], date):
        return await self._asset_repository.get_splits(date=date, assets=assets)

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

    async def get_cash_dividends_with_ex_date(self, assets, date):
        return await self._asset_repository.get_cash_dividends_with_ex_date(date=date, assets=assets)


    async def get_exchange_assets_by_sids(self, sids: list[int]) -> list[ExchangeAsset]:
        return await self._asset_repository.get_exchange_assets_by_sids(sids=sids)

    async def get_all_dividends(self, assets: list[Asset]) -> list[DividendPayout]:
        return await self._asset_repository.get_all_dividends(assets=assets)

    async def get_all_splits(self, assets: list[Asset]) -> list[Split]:
        return await self._asset_repository.get_all_splits(assets=assets)

    async def get_dividends_by_assets_and_ex_date_between(self, assets: list[Asset], ex_date_from: datetime.date,
                                                          ex_date_to: datetime.date) -> list[DividendPayout]:
        return await self._asset_repository.get_dividends_by_assets_and_ex_date_between(
            assets=assets, ex_date_from=ex_date_from, ex_date_to=ex_date_to)

    async def get_splits_by_assets_and_effective_date_between(self, assets: list[Asset],
                                                              effective_date_from: datetime.date,
                                                              effective_date_to: datetime.date) -> list[Split]:
        return await self._asset_repository.get_splits_by_assets_and_effective_date_between(
            assets=assets, effective_date_from=effective_date_from, effective_date_to=effective_date_to)

    async def preload_corporate_actions(self, assets: list[Asset],
                                        date_from: datetime.date,
                                        date_to: datetime.date) -> None:
        await self._asset_repository.preload_corporate_actions(
            assets=assets,
            date_from=date_from,
            date_to=date_to,
        )

def _rebind_bond_events(bond_events: list[BondEvent], bonds: list[Bond]) -> list[BondEvent]:
    """Point each event at the stored bond of the same identity, so it has a real asset id."""
    stored = {(bond.isin, bond.asset_name): bond for bond in bonds}
    rebound = []
    for event in bond_events:
        bond = stored.get((event.asset.isin, event.asset.asset_name))
        rebound.append(event if bond is None else dataclasses.replace(event, asset=bond))
    return rebound