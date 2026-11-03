import datetime as dt
import os
import shutil
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import pandas as pd

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.futures_root import FuturesRoot
from ziplime.assets.entities.split import Split
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.symbol_universe import SymbolsUniverse
from ziplime.assets.entities.symbols_universe_asset import SymbolsUniverseAsset
from ziplime.core.ingest_data import get_asset_service
from ziplime.errors import AmbiguousRootSymbol, SidsNotFound

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def make_currency(name: str = "USD") -> Currency:
    start, end = dt.date(2020, 1, 1), dt.date(2030, 1, 1)
    return Currency(
        id=None,
        isin=None,
        asset_name=name,
        start_date=start,
        end_date=end,
        first_traded=start,
        auto_close_date=end,
    )


def make_equity(name: str = "REPO-EQUITY", isin: str = "REPO-EQUITY-ISIN") -> Equity:
    start, end = dt.date(2020, 1, 1), dt.date(2030, 1, 1)
    return Equity(
        id=None,
        isin=isin,
        asset_name=name,
        start_date=start,
        end_date=end,
        first_traded=start,
        auto_close_date=end,
    )


class SqlAlchemyAssetRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-assets-")
        self.db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.db_path)
        self.asset_service = get_asset_service(db_path=str(self.db_path))
        self.repository = self.asset_service._asset_repository
        await type(self.repository)._compute_asset_lifetimes.cache.clear()

    async def asyncTearDown(self):
        await self.repository.engine.dispose()
        self.temp_dir.cleanup()

    async def test_exchange_queries_by_mic_and_country(self):
        exchanges = [
            ExchangeInfo("XREP", "Repository Exchange", "REP", "ZZ"),
            ExchangeInfo("XREP2", "Second Exchange", "REP2", "ZZ"),
        ]
        await self.repository.save_exchanges(exchanges)

        self.assertEqual(await self.repository.get_exchange_by_mic("XREP"), exchanges[0])
        found = await self.repository.get_exchanges_by_country_codes(frozenset({"ZZ"}))
        self.assertEqual({exchange.mic for exchange in found}, {"XREP", "XREP2"})

    async def test_currency_save_is_idempotent_and_refreshes_asset_cache(self):
        currency = make_currency()

        [first] = await self.repository.save_currencies([currency])
        [second] = await self.repository.save_currencies([currency])

        self.assertIsNotNone(first.id)
        self.assertEqual(first.id, second.id)
        self.assertEqual((await self.repository.get_all_assets())[first.id], first)

    async def test_listing_lookup_filters_by_asset_type(self):
        exchange = ExchangeInfo("XREP", "Repository Exchange", "REP", "ZZ")
        await self.repository.save_exchanges([exchange])
        [currency] = await self.repository.save_currencies([make_currency()])
        [equity] = await self.repository.save_equities([make_equity()])

        await self.repository.save_exchange_assets([
            ExchangeAsset(
                sid=None, symbol="SAME", start_date=equity.start_date,
                end_date=equity.end_date, first_traded=equity.first_traded,
                auto_close_date=equity.auto_close_date, external_id="equity",
                exchange=exchange, asset=equity, quote=currency,
            ),
            ExchangeAsset(
                sid=None, symbol="SAME", start_date=currency.start_date,
                end_date=currency.end_date, first_traded=currency.first_traded,
                auto_close_date=currency.auto_close_date, external_id="currency",
                exchange=exchange, asset=currency, quote=currency,
            ),
        ])

        equity_listing = await self.repository.get_exchange_asset_by_symbol(
            AssetSymbol("SAME", "XREP"), AssetType.EQUITY)
        currency_listing = await self.repository.get_exchange_asset_by_symbol(
            AssetSymbol("SAME", "XREP"), AssetType.CURRENCY)

        self.assertEqual(equity_listing.asset, equity)
        self.assertEqual(currency_listing.asset, currency)

    async def test_symbol_universe_is_filtered_by_date(self):
        [equity] = await self.repository.save_equities([make_equity()])
        await self.repository.save_symbol_universe(SymbolsUniverse(
            name="REPO-UNIVERSE",
            symbol="REPO",
            universe_type="equity",
            assets=[SymbolsUniverseAsset(
                symbol_universe_name="REPO-UNIVERSE",
                asset=equity,
                start_date=dt.date(2022, 1, 1),
                end_date=dt.date(2024, 1, 1),
                ratio=None,
            )],
        ))

        active = await self.repository.get_symbols_universe("REPO", dt.date(2023, 1, 1))
        inactive = await self.repository.get_symbols_universe("REPO", dt.date(2025, 1, 1))
        all_universes = await self.repository.get_all_universes()

        self.assertEqual([item.asset.id for item in active.assets], [equity.id])
        self.assertEqual(inactive.assets, [])
        self.assertEqual(all_universes["REPO"].name, "REPO-UNIVERSE")

    async def test_retrieve_all_preserves_requested_order(self):
        equities = await self.repository.save_equities([
            make_equity("FIRST", "FIRST-ISIN"),
            make_equity("SECOND", "SECOND-ISIN"),
        ])

        retrieved = await self.repository.retrieve_all([equities[1].id, equities[0].id])

        self.assertEqual([asset.asset_name for asset in retrieved], ["SECOND", "FIRST"])

    async def test_retrieve_all_handles_missing_ids(self):
        with self.assertRaises(SidsNotFound):
            await self.repository.retrieve_all([987654321])

        self.assertEqual(
            await self.repository.retrieve_all([987654321], default_none=True),
            [None],
        )

    async def test_bond_queries_cover_isins_and_exchange_listings(self):
        from ziplime.data.data_sources.demo_bonds import build_demo_bond_universe, demo_exchange

        await self.repository.save_exchanges([demo_exchange()])
        universe = build_demo_bond_universe(tickers=["ZLA27"])
        stored_bonds = await self.repository.save_bonds(universe.bonds)
        await self.repository.save_exchange_assets(universe.exchange_assets)

        [bond] = await self.repository.get_bonds_by_isins([stored_bonds[0].isin])
        listings = await self.repository.get_exchange_bonds_by_symbols(
            [AssetSymbol("ZLA27", "XNYS")])
        listing = await self.repository.get_exchange_bond_by_symbol(
            AssetSymbol("ZLA27", "XNYS"))
        all_listings = await self.repository.get_all_bond_listings(mic="XNYS")

        self.assertEqual(bond.isin, stored_bonds[0].isin)
        self.assertEqual(len(listings), 1)
        self.assertEqual(listing.sid, listings[0].sid)
        self.assertIn(listing.sid, {item.sid for item in all_listings})

    async def test_asset_and_symbol_queries_return_saved_entities(self):
        [currency] = await self.repository.save_currencies([make_currency("EUR")])
        [equity] = await self.repository.save_equities([make_equity()])

        self.assertEqual((await self.repository.get_asset_by_sid(equity.id)).id, equity.id)
        self.assertEqual(
            [asset.id for asset in await self.repository.get_assets_by_ids([currency.id, equity.id])],
            [currency.id, equity.id],
        )
        self.assertEqual(
            (await self.repository.get_currency_by_symbol("EUR")).id,
            currency.id,
        )
        self.assertEqual(
            (await self.repository.get_equity_by_symbol("REPO-EQUITY", None)).id,
            equity.id,
        )
        self.assertEqual(
            (await self.repository.get_equities_by_isins([equity.isin]))[0].id,
            equity.id,
        )

    async def test_dividends_and_splits_round_trip_and_preload(self):
        [equity] = await self.repository.save_equities([make_equity()])
        dividend = DividendPayout(
            asset=equity,
            amount=1.25,
            pay_date=dt.date(2024, 1, 10),
            declared_date=dt.date(2023, 12, 1),
            record_date=dt.date(2024, 1, 5),
            ex_date=dt.date(2024, 1, 8),
            currency=None,
        )
        split = Split(asset=equity, effective_date=dt.date(2024, 1, 9), ratio=2.0)
        await self.repository.save_dividends([dividend])
        await self.repository.save_splits([split])

        self.assertEqual(
            len(await self.repository.get_cash_dividends_with_ex_date([equity], dt.date(2024, 1, 8))),
            1,
        )
        self.assertEqual(
            len(await self.repository.get_splits([equity], dt.date(2024, 1, 9))),
            1,
        )
        await self.repository.preload_corporate_actions(
            [equity], dt.date(2024, 1, 1), dt.date(2024, 1, 31))
        self.assertEqual(
            len(await self.repository.get_all_dividends([equity])),
            1,
        )
        self.assertEqual(
            len(await self.repository.get_all_splits([equity])),
            1,
        )
        self.assertEqual(
            len(await self.repository.get_dividends_by_assets_and_ex_date_between(
                [equity], dt.date(2024, 1, 1), dt.date(2024, 1, 31))),
            1,
        )
        self.assertEqual(
            len(await self.repository.get_splits_by_assets_and_effective_date_between(
                [equity], dt.date(2024, 1, 1), dt.date(2024, 1, 31))),
            1,
        )

    async def test_repository_serializes_database_configuration(self):
        self.assertEqual(self.repository.to_json(), {"db_url": self.repository.db_url})

    async def test_futures_root_contract_and_continuous_future_queries(self):
        exchange = ExchangeInfo("XFCM", "Futures Exchange", "FCM", "ZZ")
        await self.repository.save_exchanges([exchange])
        [currency] = await self.repository.save_currencies([make_currency()])
        start, end = dt.date(2023, 1, 1), dt.date(2024, 12, 31)
        [root_asset] = await self.repository.save_commodities([Commodity(
            id=None, isin=None, asset_name="CL", start_date=start, end_date=end,
            first_traded=start, auto_close_date=end)])
        root = FuturesRoot(
            root_symbol="CL", description="Crude Oil", exchange=exchange,
            root_asset=root_asset, multiplier=1000.0, tick_size=0.01,
            quote_currency=currency.asset_name, margin_currency=currency.asset_name,
        )
        await self.repository.save_futures_roots([root])
        contracts = await self.repository.save_futures_contracts([
            FuturesContract(
                id=None, isin=None, asset_name="CLM23", start_date=start, end_date=dt.date(2023, 6, 20),
                first_traded=start, auto_close_date=dt.date(2023, 6, 21),
                root_exchange_asset=None, root_asset=root_asset, root_symbol="CL",
                notice_date=dt.date(2023, 6, 19), expiration_date=dt.date(2023, 6, 20),
                multiplier=1000.0, tick_size=0.01, settlement_type=SettlementType.CASH,
                margin_currency=currency.asset_name,
            ),
            FuturesContract(
                id=None, isin=None, asset_name="CLZ23", start_date=start, end_date=dt.date(2023, 12, 20),
                first_traded=start, auto_close_date=dt.date(2023, 12, 21),
                root_exchange_asset=None, root_asset=root_asset, root_symbol="CL",
                notice_date=dt.date(2023, 12, 19), expiration_date=dt.date(2023, 12, 20),
                multiplier=1000.0, tick_size=0.01, settlement_type=SettlementType.CASH,
                margin_currency=currency.asset_name,
            ),
        ])
        listings = await self.repository.save_exchange_assets([
            ExchangeAsset(
                sid=None, symbol=contract.asset_name, start_date=contract.start_date,
                end_date=contract.end_date, first_traded=contract.first_traded,
                auto_close_date=contract.auto_close_date, external_id=contract.asset_name,
                exchange=exchange, asset=contract, quote=currency,
            )
            for contract in contracts
        ])

        roots = await self.repository.get_futures_roots()
        by_symbol = await self.repository.get_exchange_futures_contracts_by_symbols(
            [AssetSymbol("CLM23", "XFCM")])
        single = await self.repository.get_exchange_futures_contract_by_symbol(
            AssetSymbol("CLM23", "XFCM"))
        contract = await self.repository.get_futures_contract_by_symbol("CLM23", "XFCM")
        by_root = await self.repository.get_exchange_futures_contracts_by_root("CL", "XFCM")
        ordered = await self.repository.get_ordered_contracts("CL", "XFCM")
        continuous = await self.repository.create_continuous_future("CL", 0, "volume", "mul", mic="XFCM")
        spelled = await self.repository.create_continuous_future("CL@XFCM", 0, "volume", "mul")
        nymex = await self.repository.create_continuous_future("CL@XNYM", 0, "volume", "mul")

        # The seed database already holds NYMEX CL. A second CL on another exchange used to be
        # skipped as already stored, its contracts filed under NYMEX's specification.
        self.assertEqual(roots[("CL", "XFCM")].root_symbol, "CL")
        self.assertIn(("CL", "XNYM"), roots)
        with self.assertRaises(AmbiguousRootSymbol):
            await self.repository.create_continuous_future("CL", 0, "volume", "mul")
        self.assertEqual(spelled.sid, continuous.sid)
        self.assertNotEqual(nymex.sid, continuous.sid)
        self.assertEqual(continuous.mic, "XFCM")
        self.assertEqual(by_symbol[0].sid, listings[0].sid)
        self.assertEqual(single.sid, listings[0].sid)
        self.assertEqual(contract.id, contracts[0].id)
        self.assertEqual([item.sid for item in by_root], [listings[0].sid, listings[1].sid])
        self.assertEqual([item.sid for item in ordered.contracts], [listings[0].sid, listings[1].sid])
        self.assertEqual(continuous.root_symbol, "CL")
        self.assertEqual(continuous.offset, 0)

    async def _store_chain(self, exchange: ExchangeInfo, root_symbol: str, multiplier: float,
                           deliveries: list[tuple[str, str, dt.date]]) -> list[ExchangeAsset]:
        """One chain on one exchange: ``deliveries`` is ``[(asset_name, symbol, expiry), ...]``."""
        [currency] = await self.repository.save_currencies([make_currency()])
        far_past, far_future = dt.date(2000, 1, 1), dt.date(2040, 1, 1)
        [underlying] = await self.repository.save_commodities([Commodity(
            id=None, isin=None, asset_name=f"{root_symbol}@{exchange.mic}", start_date=far_past,
            end_date=far_future, first_traded=far_past, auto_close_date=far_future)])
        await self.repository.save_futures_roots([FuturesRoot(
            root_symbol=root_symbol, description=root_symbol, exchange=exchange,
            root_asset=underlying, multiplier=multiplier, tick_size=0.001,
            quote_currency=currency.asset_name, margin_currency=currency.asset_name)])
        contracts = await self.repository.save_futures_contracts([
            FuturesContract(
                id=None, isin=None, asset_name=name, start_date=expiry - dt.timedelta(days=400),
                end_date=expiry, first_traded=expiry - dt.timedelta(days=400),
                auto_close_date=expiry + dt.timedelta(days=1), root_exchange_asset=None,
                root_asset=underlying, root_symbol=root_symbol, notice_date=expiry,
                expiration_date=expiry, multiplier=multiplier, tick_size=0.001,
                settlement_type=SettlementType.CASH, margin_currency=currency.asset_name)
            for name, _, expiry in deliveries])
        return await self.repository.save_exchange_assets([
            ExchangeAsset(
                sid=None, symbol=symbol, start_date=contract.start_date, end_date=contract.end_date,
                first_traded=contract.first_traded, auto_close_date=contract.auto_close_date,
                external_id=symbol, exchange=exchange, asset=contract, quote=currency)
            for contract, (_, symbol, _) in zip(contracts, deliveries)])

    async def test_one_root_on_two_exchanges_keeps_two_chains(self):
        moscow = ExchangeInfo("XMOF", "Moscow futures", "MOF", "RU")
        nymex = ExchangeInfo("XNYF", "NYMEX futures", "NYF", "US")
        await self.repository.save_exchanges([moscow, nymex])
        moscow_ng = await self._store_chain(moscow, "QG", 100.0, [
            ("QGX6", "QGX6", dt.date(2026, 11, 25)), ("QGZ6", "QGZ6", dt.date(2026, 12, 23))])
        nymex_ng = await self._store_chain(nymex, "QG", 10_000.0, [
            ("QGZ26", "QGZ26", dt.date(2026, 11, 25))])

        roots = await self.repository.get_futures_roots()
        self.assertEqual(roots[("QG", "XMOF")].multiplier, 100.0)
        self.assertEqual(roots[("QG", "XNYF")].multiplier, 10_000.0)

        moscow_chain = await self.repository.get_exchange_futures_contracts_by_root("QG@XMOF")
        nymex_chain = await self.repository.get_exchange_futures_contracts_by_root("QG", mic="XNYF")
        self.assertEqual([c.sid for c in moscow_chain], [c.sid for c in moscow_ng])
        self.assertEqual([c.sid for c in nymex_chain], [c.sid for c in nymex_ng])
        self.assertEqual(nymex_chain[0].asset.multiplier, 10_000.0)

        with self.assertRaises(AmbiguousRootSymbol):
            await self.repository.get_exchange_futures_contracts_by_root("QG")
        with unittest.mock.patch.dict(os.environ, {"ZIPLIME_FUTURES_DEFAULT_MIC": "XLON,XMOF"}):
            preferred = await self.repository.get_exchange_futures_contracts_by_root("QG")
        self.assertEqual([c.sid for c in preferred], [c.sid for c in moscow_ng])

    async def test_a_reused_ticker_is_two_contracts(self):
        # The Moscow Exchange writes the year as one digit, so SiH4 is both the March 2014 and the
        # March 2024 delivery. The second used to be dropped as already stored.
        exchange = ExchangeInfo("XMOR", "Moscow futures", "MOR", "RU")
        await self.repository.save_exchanges([exchange])
        listings = await self._store_chain(exchange, "Sx", 1.0, [
            ("SxH4", "SxH14", dt.date(2014, 3, 17)), ("SxH4", "SxH4", dt.date(2024, 3, 21))])

        chain = await self.repository.get_exchange_futures_contracts_by_root("Sx")
        self.assertEqual(len({c.asset.id for c in chain}), 2)
        self.assertEqual([c.asset.expiration_date for c in chain],
                         [dt.date(2014, 3, 17), dt.date(2024, 3, 21)])
        self.assertEqual([c.symbol for c in chain], [listing.symbol for listing in listings])

    async def test_generic_listing_dispatch_and_lifetimes(self):
        exchange = ExchangeInfo("XLIFE", "Lifetime Exchange", "LIFE", "ZZ")
        await self.repository.save_exchanges([exchange])
        [currency] = await self.repository.save_currencies([make_currency()])
        [equity] = await self.repository.save_equities([make_equity()])
        [listing] = await self.repository.save_exchange_assets([ExchangeAsset(
            sid=None, symbol="LIFE", start_date=equity.start_date, end_date=equity.end_date,
            first_traded=equity.first_traded, auto_close_date=equity.auto_close_date,
            external_id="life", exchange=exchange, asset=equity, quote=currency)])

        found = await self.repository.get_exchange_assets_by_symbols_of_type(
            [AssetSymbol("LIFE", "XLIFE")], AssetType.EQUITY)
        direct = await self.repository.get_equities_by_symbols_and_exchange(["LIFE"], "XLIFE")
        by_sid = await self.repository.get_exchange_assets_by_sids([listing.sid])
        lifetimes = await self.repository.asset_lifetimes(
            [listing], pd.date_range("2024-01-01", periods=2, freq="D"), True)

        self.assertEqual(found[0].sid, listing.sid)
        self.assertEqual(direct[0].id, equity.id)
        self.assertEqual(by_sid[0].sid, listing.sid)
        self.assertEqual(list(lifetimes.sid), [listing.sid])
        with self.assertRaises(ValueError):
            await self.repository.get_exchange_asset_by_symbol(
                AssetSymbol("LIFE", "XLIFE"), AssetType.COMMODITY)


if __name__ == "__main__":
    unittest.main(verbosity=2)
