"""Historical-cost investment PNL and same-bar order comparisons, with no external data.

Run from the repository root:

    python -m examples.currencies.run_investment_pnl

Assets live in a temporary SQLite database; FX bars are written to Delta Lake and read with
Polars through CurrencyConversionService. Everything is synthetic, including SBER's listing
on the demo's single XNYS calendar. No credentials or checked-in database are used.

The strategy buys EUR 10,000 of SBER and USD 150 of a USD-quoted stock. SBER rises 10%, but
USD/EUR moves from 82 to 200: SBER gains EUR 1,000 while losing USD 66.9512. After selling,
USD/EUR reaches 220; the retained EUR cash loses another USD 5, without changing security PNL.
The final session demonstrates external funding and an explicit executed FX cash exchange.
"""
import asyncio
import datetime
import math
import tempfile
from dataclasses import replace
from pathlib import Path

import pandas as pd
import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.data.services.file_system_delta_lake_bundle_storage import FileSystemDeltaLakeBundleStorage
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
from ziplime.utils.calendar_utils import get_calendar

HERE = Path(__file__).parent
DAY = datetime.timedelta(days=1)
FIRST = datetime.date(year=2024, month=1, day=2)
LAST = datetime.date(year=2024, month=1, day=8)
LISTED_FROM = datetime.date(year=2020, month=1, day=1)
LISTED_UNTIL = datetime.date(year=2030, month=1, day=1)


async def seed_assets(service: AssetService) -> tuple[ExchangeAsset, ExchangeAsset, ExchangeAsset]:
    """Persist the demo's synthetic currency and equity identities.

    Parameters
    ----------
    service : AssetService
        Service connected to the empty temporary assets database.

    Returns
    -------
    tuple[ExchangeAsset, ExchangeAsset, ExchangeAsset]
        Persisted SBER/EUR, USD_STOCK/USD, and USD/EUR listings, in that order.
        All use the demo's XNYS venue/calendar, not real Moscow trading hours.
    """
    exchange = ExchangeInfo(mic="XNYS", name="Demo", canonical_name="Demo", country_code="US")
    await service.save_exchanges(exchanges=[exchange])
    eur, usd = await service.save_currencies(currencies=[
        Currency(
            id=None, isin=None, asset_name=code, start_date=LISTED_FROM, end_date=LISTED_UNTIL,
            first_traded=LISTED_FROM, auto_close_date=LISTED_UNTIL)
        for code in ("EUR", "USD")
    ])
    equities = await service.save_equities(equities=[
        Equity(
            id=None, isin=None, asset_name=symbol, start_date=LISTED_FROM, end_date=LISTED_UNTIL,
            first_traded=LISTED_FROM, auto_close_date=LISTED_UNTIL)
        for symbol in ("SBER", "USD_STOCK")
    ])
    sber, usd_stock, fx = await service.save_exchange_assets(exchange_assets=[
        ExchangeAsset(
            sid=None, symbol=symbol, asset=asset, quote=quote, exchange=exchange,
            start_date=LISTED_FROM, end_date=LISTED_UNTIL, first_traded=LISTED_FROM,
            auto_close_date=LISTED_UNTIL, external_id=symbol)
        for symbol, asset, quote in zip(
            ("SBER", "USD_STOCK", "USD_EUR"), [*equities, usd], (eur, usd, eur), strict=True)
    ])
    return sber, usd_stock, fx


def make_bundle(name: str, data: pl.DataFrame, calendar: ExchangeCalendar) -> DataBundle:
    """Build a daily bundle with explicit calendar timezone and per-listing row spans.

    Parameters
    ----------
    name : str
        Bundle name, also used by Delta storage for the FX bundle.
    data : polars.DataFrame
        Nonempty synthetic bars containing ``sid`` and aware ``date`` columns.
    calendar : ExchangeCalendar
        Session calendar and timezone to normalize availability timestamps against.

    Returns
    -------
    DataBundle
        SID/date-sorted daily data with exclusive-end ``sid_indexes``.
    """
    data = data.with_columns(pl.col("date").dt.convert_time_zone(str(calendar.tz))).sort(["sid", "date"])
    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        pl.col("index").first().alias("first"), pl.col("index").last().alias("last"))
    return DataBundle(
        name=name, version="1", start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=DAY, original_frequency=DAY,
        data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
        sid_indexes={row["sid"]: (row["first"], row["last"] + 1) for row in spans.iter_rows(named=True)})


async def run_example(workspace: Path) -> TradingAlgorithmExecutionResult:
    """Run the mixed-currency demo and verify its historical-PNL reconciliation.

    Parameters
    ----------
    workspace : pathlib.Path
        Existing empty caller-owned directory for the asset database and Delta
        files. The caller owns directory cleanup; database engines are disposed here.

    Returns
    -------
    TradingAlgorithmExecutionResult
        Five daily sessions with EUR/USD metrics, recorded pre-metric order/PNL
        estimates, and exportable native investment history.

    Raises
    ------
    RuntimeError
        If the simulation reports execution errors.
    AssertionError
        If history replay, cash FX, or deposit-excluded PNL differs from the
        deterministic worked example. No network requests or real data are used.
    """
    service = get_asset_service(db_path=str(workspace / "assets.sqlite"))
    try:
        calendar = get_calendar("XNYS")
        sessions = calendar.sessions_in_range(start=FIRST, end=LAST)
        initial = calendar.session_open(session=sessions[0]).to_pydatetime()
        closes = [calendar.session_close(session=session).to_pydatetime() for session in sessions]
        sber, usd_stock, fx_listing = await seed_assets(service=service)

        # A quote is EUR per USD. Seed the initial open, then publish changes at session close.
        fx_data = pl.DataFrame(data={
            "sid": [fx_listing.sid] * 6,
            "date": [initial - DAY, *closes],
            "close": [82.0, 82.0, 200.0, 200.0, 220.0, 220.0],
        })
        fx_bundle = make_bundle(name="demo-fx", data=fx_data, calendar=calendar)
        storage = FileSystemDeltaLakeBundleStorage(base_data_path=str(workspace / "delta"))
        await storage.store_bundle(data_bundle=fx_bundle, merge=False)
        selected_fx = await service.get_exchange_currency_by_symbol(symbol=AssetSymbol(symbol="USD_EUR", mic="XNYS"))
        if selected_fx is None:
            raise ValueError("The demo FX listing was not persisted.")
        conversion = await CurrencyConversionService.from_bundle(
            storage=storage,
            bundle=fx_bundle,
            listings=[selected_fx],
            start=initial,
            end=closes[-1],
            max_rate_age=DAY * 7,
        )
        reporting = CurrencyReporting(
            base_currency="EUR", currencies=("EUR", "USD"), conversion=conversion, investment_pnl=True,
            opening_cash={
                ("LIME", "simulation_account", "EUR"): 10_000.0,
                ("LIME", "simulation_account", "USD"): 150.0,
            })
        market_data = pl.concat(items=[
            pl.DataFrame(data={
                "sid": [asset.sid] * len(closes), "date": closes,
                **{field: prices for field in ("open", "high", "low", "close")},
                "volume": [1_000_000.0] * len(closes),
            })
            for asset, prices in (
                (sber, [100.0, 110.0, 110.0, 110.0, 110.0]),
                (usd_stock, [30.0] * len(closes)))
        ])
        result = await run_simulation(
            start_date=datetime.datetime.combine(date=FIRST, time=datetime.time.min, tzinfo=calendar.tz),
            end_date=datetime.datetime.combine(date=LAST, time=datetime.time.max, tzinfo=calendar.tz),
            trading_calendar="XNYS", emission_rate=DAY, total_cash=10_000 + 150 * 82,
            market_data_source=make_bundle(
                name="demo-stocks",
                data=market_data,
                calendar=calendar,
            ), custom_data_sources=[],
            algorithm_file=str(HERE / "strategy.py"), stop_on_error=True, asset_service=service,
            equity_commission=NoCommission(), equity_slippage=NoSlippage(), print_algo=False,
            currency_reporting=reporting,
            # Synthetic same-close fills make the arithmetic exact, not a realistic execution model.
            same_bar_execution=True,
        )
        if result.errors:
            raise RuntimeError(f"The example failed: {result.errors}")

        print("\nPortfolio PNL is daily; investment fields and cash_fx_pnl are cumulative:")
        columns = ["pnl", "investment_realized_pnl", "investment_unrealized_pnl", "cash_fx_pnl", "ending_cash"]
        for code, performance in result.perf_by_currency.items():
            table = performance[columns].rename(columns={"pnl": "daily_portfolio_pnl"})
            print(f"\n{code}\n{table.to_string(float_format=lambda value: f'{value:,.4f}')}")

        # Domain-event export/restore is separate from serialization and execution-position restoration.
        history = result.trading_algorithm.investment_history()
        restored = Ledger(pd.DatetimeIndex(closes), DAY)
        for position in result.trading_algorithm._ledger.position_tracker.get_position_list():
            restored.position_tracker.update_position(
                asset=position.asset, exchange_name=position.exchange_name,
                trading_account_id=position.trading_account_id, amount=position.amount,
                cost_basis=position.cost_basis, last_sale_price=position.last_sale_price,
                last_sale_date=position.last_sale_date)
        restored.enable_investment_accounting(
            reporting=replace(reporting, investment_history=history), dt=closes[-1], opening_cash={})
        snapshot = restored.investment_snapshot(
            currency="USD", dt=closes[-1], marks={("LIME", "simulation_account", usd_stock): (30.0, closes[-1])})
        usd = result.perf_by_currency["USD"]
        expected_security_pnl = 55 - 10_000 / 82
        if not math.isclose(a=snapshot.total_pnl, b=expected_security_pnl, abs_tol=1e-9):
            raise AssertionError("Restored historical investment PNL did not match the SBER example.")
        if not math.isclose(a=usd["cash_fx_pnl"].iloc[-1], b=-5.0, abs_tol=1e-9):
            raise AssertionError("The post-sale cash FX loss must be separate from security PNL.")
        if not math.isclose(a=usd["pnl"].sum(), b=expected_security_pnl - 5.0, abs_tol=1e-9):
            raise AssertionError("Portfolio PNL must include cash FX but exclude the external deposit.")
        print(f"\nRestored security PNL: USD {snapshot.total_pnl:+.4f}")
        print(f"Total portfolio PNL: USD {usd['pnl'].sum():+.4f}")
        print(f"Native cash after funding and FX execution: {restored.cash_balances(dt=closes[-1])}")
        print("The USD 25 deposit is excluded from PNL; FX execution moves cash, not security cost basis.")
        return result
    finally:
        await service._asset_repository.engine.dispose()


async def main() -> None:
    """Run the demo with automatic cleanup of its temporary assets and Delta files."""
    with tempfile.TemporaryDirectory(prefix="ziplime-currencies-") as workspace:
        await run_example(workspace=Path(workspace))


if __name__ == "__main__":
    asyncio.run(main=main())
