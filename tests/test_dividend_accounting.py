from __future__ import annotations

import datetime as dt
import contextlib
import io
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import polars as pl
from iso4217 import Currency as ISO4217Currency

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.currency import Currency as CurrencyAsset
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.domain.portfolio import Portfolio
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.position_tracker import PositionTracker
from ziplime.utils.calendar_utils import get_calendar


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_exchange_asset(sid: int = 101) -> ExchangeAsset:
    first_day = dt.date(2020, 1, 1)
    last_day = dt.date(2030, 1, 1)
    underlying = Equity(
        id=1,
        isin="TEST00000001",
        asset_name="TEST",
        start_date=first_day,
        end_date=last_day,
        first_traded=first_day,
        auto_close_date=last_day,
    )
    quote = CurrencyAsset(
        id=2,
        isin=None,
        asset_name="USD",
        start_date=first_day,
        end_date=last_day,
        first_traded=first_day,
        auto_close_date=last_day,
    )
    return ExchangeAsset(
        sid=sid,
        symbol="TEST",
        start_date=first_day,
        end_date=last_day,
        first_traded=first_day,
        auto_close_date=last_day,
        external_id=str(sid),
        exchange=ExchangeInfo(
            mic="XNAS",
            name="LIME",
            canonical_name="NASDAQ",
            country_code="US",
        ),
        asset=underlying,
        quote=quote,
    )


def make_dividend(
    asset: Equity,
    *,
    amount: float = 1.0,
    ex_date: dt.date = dt.date(2025, 1, 6),
    pay_date: dt.date = dt.date(2025, 1, 10),
) -> DividendPayout:
    return DividendPayout(
        asset=asset,
        amount=amount,
        declared_date=ex_date - dt.timedelta(days=20),
        record_date=ex_date + dt.timedelta(days=1),
        ex_date=ex_date,
        pay_date=pay_date,
        currency=ISO4217Currency.USD,
    )


class InMemoryDividendService:
    def __init__(self, dividends: list[DividendPayout]):
        self._dividends = dividends

    async def get_cash_dividends_with_ex_date(self, assets, date):
        asset_ids = {asset.id for asset in assets}
        return [
            dividend
            for dividend in self._dividends
            if dividend.asset.id in asset_ids and dividend.ex_date == date
        ]


class PositionTrackerDividendTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.asset = make_exchange_asset()
        self.ex_date = dt.date(2025, 1, 6)
        self.pay_date = dt.date(2025, 1, 10)

    def tracker_with_position(self, amount: int) -> PositionTracker:
        tracker = PositionTracker(data_frequency=dt.timedelta(days=1))
        tracker.update_position(
            asset=self.asset,
            exchange_name="LIME",
            trading_account_id="account-1",
            amount=amount,
            last_sale_price=100.0,
        )
        return tracker

    def test_single_long_position_is_paid_once(self):
        tracker = self.tracker_with_position(10)
        dividend = make_dividend(
            self.asset.asset,
            amount=1.25,
            ex_date=self.ex_date,
            pay_date=self.pay_date,
        )

        tracker.earn_dividends([dividend], [])

        self.assertEqual(tracker.pay_dividends(self.pay_date), 12.5)
        self.assertEqual(tracker.pay_dividends(self.pay_date), 0.0)

    def test_sale_after_ex_date_does_not_remove_entitlement(self):
        tracker = self.tracker_with_position(10)
        dividend = make_dividend(
            self.asset.asset,
            ex_date=self.ex_date,
            pay_date=self.pay_date,
        )

        tracker.earn_dividends([dividend], [])
        tracker.positions.clear()

        self.assertEqual(tracker.pay_dividends(self.pay_date), 10.0)

    @unittest.expectedFailure
    def test_positions_across_accounts_are_aggregated(self):
        tracker = self.tracker_with_position(10)
        tracker.update_position(
            asset=self.asset,
            exchange_name="LIME",
            trading_account_id="account-2",
            amount=20,
            last_sale_price=100.0,
        )
        dividend = make_dividend(
            self.asset.asset,
            ex_date=self.ex_date,
            pay_date=self.pay_date,
        )

        tracker.earn_dividends([dividend], [])

        self.assertEqual(tracker.pay_dividends(self.pay_date), 30.0)

    async def test_portfolio_helper_aggregates_positions_across_accounts(self):
        tracker = self.tracker_with_position(10)
        tracker.update_position(
            asset=self.asset,
            exchange_name="LIME",
            trading_account_id="account-2",
            amount=20,
            last_sale_price=100.0,
        )
        portfolio = Portfolio(
            cash_flow=0.0,
            starting_cash=1_000.0,
            portfolio_value=4_000.0,
            pnl=0.0,
            returns=0.0,
            cash=1_000.0,
            positions_value=3_000.0,
            positions_exposure=3_000.0,
            positions=tracker.get_positions(),
        )

        amount = await portfolio.get_asset_positions_amount(self.asset)

        self.assertEqual(amount, 30)


class LedgerDividendTests(unittest.IsolatedAsyncioTestCase):
    @unittest.expectedFailure
    async def test_short_dividend_is_applied_as_negative_cash_flow(self):
        ex_date = dt.date(2025, 1, 6)
        pay_date = dt.date(2025, 1, 10)
        asset = make_exchange_asset()
        dividend = make_dividend(
            asset.asset,
            amount=1.0,
            ex_date=ex_date,
            pay_date=pay_date,
        )
        ledger = Ledger(
            trading_sessions=pd.DatetimeIndex([ex_date, pay_date]),
            data_frequency=dt.timedelta(days=1),
        )
        ledger._portfolio.cash = 1_000.0
        ledger.position_tracker.update_position(
            asset=asset,
            exchange_name="LIME",
            trading_account_id="account-1",
            amount=-10,
            last_sale_price=100.0,
        )
        service = InMemoryDividendService([dividend])

        await ledger.process_dividends(ex_date, service)
        await ledger.process_dividends(pay_date, service)

        self.assertEqual(ledger.portfolio.cash, 990.0)
        self.assertEqual(ledger.portfolio.cash_flow, -10.0)

    async def test_single_long_dividend_reaches_ledger_cash(self):
        ex_date = dt.date(2025, 1, 6)
        pay_date = dt.date(2025, 1, 10)
        asset = make_exchange_asset()
        dividend = make_dividend(
            asset.asset,
            amount=1.0,
            ex_date=ex_date,
            pay_date=pay_date,
        )
        ledger = Ledger(
            trading_sessions=pd.DatetimeIndex([ex_date, pay_date]),
            data_frequency=dt.timedelta(days=1),
        )
        ledger._portfolio.cash = 1_000.0
        ledger.position_tracker.update_position(
            asset=asset,
            exchange_name="LIME",
            trading_account_id="account-1",
            amount=10,
            last_sale_price=100.0,
        )
        service = InMemoryDividendService([dividend])

        await ledger.process_dividends(ex_date, service)
        await ledger.process_dividends(pay_date, service)

        self.assertEqual(ledger.portfolio.cash, 1_010.0)
        self.assertEqual(ledger.portfolio.cash_flow, 10.0)


class DividendRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="ziplime-dividend-test-")
        self.db_path = Path(self.temp_dir.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.db_path)
        self.asset_service = get_asset_service(db_path=str(self.db_path))
        assets = await self.asset_service._asset_repository.get_all_assets()
        self.asset = next(asset for asset in assets.values() if isinstance(asset, Equity))

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self.temp_dir.cleanup()

    @unittest.expectedFailure
    async def test_import_preserves_ex_date(self):
        ex_date = dt.date(2025, 1, 6)
        pay_date = dt.date(2025, 1, 20)
        payout = make_dividend(
            self.asset,
            ex_date=ex_date,
            pay_date=pay_date,
        )

        await self.asset_service.import_dividends([payout])

        with sqlite3.connect(self.db_path) as connection:
            stored_ex_date, stored_pay_date = connection.execute(
                "SELECT ex_date, pay_date FROM dividend_payouts ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertEqual(stored_ex_date, ex_date.isoformat())
        self.assertEqual(stored_pay_date, pay_date.isoformat())

    @unittest.expectedFailure
    async def test_repeated_import_is_idempotent(self):
        payout = make_dividend(self.asset)

        await self.asset_service.import_dividends([payout])
        await self.asset_service.import_dividends([payout])

        with sqlite3.connect(self.db_path) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM dividend_payouts WHERE asset_id = ? AND amount = ?",
                (self.asset.id, payout.amount),
            ).fetchone()[0]
        self.assertEqual(count, 1)


class DividendEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def run_case(self, db_path: Path, include_dividend: bool):
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
        asset_service = get_asset_service(db_path=str(db_path))
        asset = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="SBER", mic="MISX"),
            asset_type=AssetType.EQUITY,
        )
        self.assertIsNotNone(asset)

        if include_dividend:
            # Seed a correctly normalized payout directly. The public import
            # path is covered separately and currently corrupts ex_date.
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO dividend_payouts
                        (asset_id, ex_date, declared_date, record_date, pay_date, amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asset.asset.id,
                        "2024-07-10",
                        "2024-06-20",
                        "2024-07-11",
                        "2024-07-19",
                        33.3,
                    ),
                )

        calendar = get_calendar("XMOS")
        start_date = dt.datetime(2024, 7, 8, tzinfo=calendar.tz)
        end_date = dt.datetime(2024, 7, 22, 23, 59, tzinfo=calendar.tz)
        sessions = calendar.sessions_in_range(start_date.date(), end_date.date())
        closes = list(
            calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz)
        )
        row_count = len(closes)
        data = pl.DataFrame(
            {
                "date": closes,
                "sid": [asset.sid] * row_count,
                "symbol": [asset.symbol] * row_count,
                "mic": [asset.mic] * row_count,
                "open": [100.0] * row_count,
                "high": [100.0] * row_count,
                "low": [100.0] * row_count,
                "close": [100.0] * row_count,
                "price": [100.0] * row_count,
                "volume": [1_000_000.0] * row_count,
            }
        )
        bundle = DataBundle(
            name="dividend-e2e",
            version="1",
            start_date=start_date,
            end_date=end_date,
            trading_calendar=calendar,
            frequency=dt.timedelta(days=1),
            original_frequency=dt.timedelta(days=1),
            data_type=DataType.MARKET_DATA,
            timestamp=dt.datetime.now(tz=calendar.tz),
            data=data,
            sid_indexes={asset.sid: (0, row_count)},
        )

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = await run_simulation(
                start_date=start_date,
                end_date=end_date,
                trading_calendar="XMOS",
                emission_rate=dt.timedelta(days=1),
                total_cash=100_000.0,
                market_data_source=bundle,
                custom_data_sources=[],
                algorithm_file=str(
                    PROJECT_ROOT / "tests" / "fixtures" / "dividend_buy_then_sell.py"
                ),
                stop_on_error=True,
                asset_service=asset_service,
                benchmark_asset_symbol="SBER@MISX",
                benchmark_returns=None,
                equity_commission=NoCommission(),
                max_leverage=1.0,
                same_bar_execution=True,
                price_used_in_order_execution="close",
            )

        final_cash = result.trading_algorithm.portfolio.cash
        errors = result.errors
        await asset_service._asset_repository.engine.dispose()
        return final_cash, errors

    async def test_full_backtest_preserves_ex_date_entitlement(self):
        with tempfile.TemporaryDirectory(prefix="ziplime-dividend-e2e-") as temp_dir:
            temp_path = Path(temp_dir)
            control_cash, control_errors = await self.run_case(
                temp_path / "control.sqlite", include_dividend=False
            )
            dividend_cash, dividend_errors = await self.run_case(
                temp_path / "dividend.sqlite", include_dividend=True
            )

        self.assertEqual(control_errors, [])
        self.assertEqual(dividend_errors, [])
        self.assertAlmostEqual(dividend_cash - control_cash, 333.0, places=6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
