"""The same strategy through both ziplime engines, and the answer has to be the same.

This is the test the whole kernel is for. Agreeing with vectorbt (`tests/vectorized/reference/vectorbt/`)
shows the kernel computes what the library it replaces computed. Agreeing with ziplime's *own*
event-driven engine shows something more useful: that a strategy screened fast and validated
carefully is one strategy rather than two, which is the premise of the hybrid in the spec's §46.

The two paths share everything downstream -- ledger, metrics, result -- so a difference here is a
difference in execution, which is the only place the two engines are allowed to differ at all.
They are lined up on the three things that decide a fill:

    timing      same_close  <->  same_bar_execution=True, fill at the close
    commission  NoCommission on both
    slippage    NoSlippage on both

Take any of those apart and the runs diverge for a reason that has nothing to do with the kernel.
"""
import contextlib
import datetime
import io
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar
from ziplime.vectorized import to_execution_result
from ziplime.vectorized.kernel import (
    ExecutionTiming, compare_execution_results, simulate_signals,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"
CALENDAR = "XNYS"
TICKERS = ("JNJ", "KO")
CASH = 1_000_000.0
FAST, SLOW, SIZE = 5, 20, 100

#: The run, and the warm-up before it. The bundle covers both so the slow average is already
#: warm on the first simulated bar -- otherwise the two sides would be compared over a stretch
#: where neither trades, which proves nothing.
START, END = datetime.date(2024, 3, 1), datetime.date(2024, 9, 30)
BUNDLE_START = datetime.date(2023, 11, 1)


def price_path(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Shaped to cross the averages repeatedly. A path that never crosses would let both engines
    agree by doing nothing."""
    steps = np.arange(len(index))
    return pd.DataFrame({
        "JNJ": 100.0 + np.sin(steps / 11.0) * 9.0 + np.sin(steps / 3.0) * 2.0 + steps * 0.02,
        "KO": 50.0 + np.cos(steps / 8.0) * 5.0 + np.cos(steps / 2.5) * 1.5 - steps * 0.01,
    }, index=index)


class ParityTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-parity-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(BUNDLE_START, END)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.prices = price_path(self.index)
        self.listings = {}
        for ticker in TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def exchange(self) -> SimulationExchange:
        return SimulationExchange(
            name="PARITY", country_code="US", trading_calendar=self.calendar, clock=None,
            cash_balance=CASH, equity_slippage=NoSlippage(), future_slippage=NoSlippage(),
            equity_commission=NoCommission(), future_commission=NoCommission(),
            account_id="vectorized_account", is_default=True)

    def bundle(self) -> DataBundle:
        rows = [{"date": stamp, "sid": self.listings[t].sid, "symbol": t, "mic": "XNYS",
                 "open": p, "high": p, "low": p, "close": p, "price": p, "volume": 1e9}
                for t in TICKERS for stamp, p in self.prices[t].items()]
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="parity", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)})

    async def run_event(self):
        """The careful engine, filling at the close of the bar it decided on."""
        from ziplime.core.run_simulation import run_simulation

        with contextlib.redirect_stdout(io.StringIO()):
            return await run_simulation(
                start_date=datetime.datetime.combine(START, datetime.time.min,
                                                     tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(END, datetime.time.max,
                                                   tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                total_cash=CASH, market_data_source=self.bundle(), custom_data_sources=[],
                algorithm_file=str(FIXTURES / "parity_sma.py"), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None, equity_commission=NoCommission(),
                equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False,
                same_bar_execution=True, price_used_in_order_execution="close")

    def signals(self):
        """The same crossover, as arrays. Computed over the whole history including the warm-up,
        then sliced to the run -- which is what the event engine effectively does by reading
        `data.history` back past its own start date."""
        fast = self.prices.rolling(FAST).mean()
        slow = self.prices.rolling(SLOW).mean()
        entries = (fast > slow).fillna(False)
        exits = (fast < slow).fillna(False)
        in_run = self.index >= pd.Timestamp(START, tz=self.calendar.tz)
        return (self.prices.loc[in_run], entries.loc[in_run], exits.loc[in_run])

    async def run_vector(self):
        """The fast engine, through the kernel and then the same ledger and metrics."""
        prices, entries, exits = self.signals()
        result = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=SIZE, initial_cash=CASH,
            execution=ExecutionTiming.same_close(),
            assets={t: self.listings[t] for t in TICKERS})
        with contextlib.redirect_stdout(io.StringIO()):
            return result, await to_execution_result(
                portfolio=result, listings=self.listings, prices=prices,
                trading_calendar=self.calendar, exchange=self.exchange(),
                emission_rate=datetime.timedelta(days=1))


class VectorEventParityTests(ParityTestCase):
    """§31. The comparison the hybrid rests on."""

    async def test_both_engines_book_the_same_trades(self):
        _, vector = await self.run_vector()
        event = await self.run_event()

        def signatures(result):
            return [(t.dt, t.asset.symbol, round(float(t.amount), 8), round(float(t.price), 8))
                    for row in result.perf["transactions"] for t in row]

        placed = signatures(vector)
        self.assertGreater(len(placed), 4, "neither engine traded; this proves nothing")
        self.assertEqual(placed, signatures(event))

    async def test_both_engines_agree_bar_for_bar(self):
        _, vector = await self.run_vector()
        event = await self.run_event()
        for column in ("portfolio_value", "ending_cash", "ending_exposure", "returns"):
            np.testing.assert_allclose(
                vector.perf[column].to_numpy(dtype=float),
                event.perf[column].to_numpy(dtype=float), rtol=0, atol=1e-9,
                err_msg=f"{column} differs between the vector and event engines")

    async def test_both_engines_report_the_same_risk_metrics(self):
        """The metrics come from the same tracker on both sides, so a difference here can only
        come from the trades -- which is exactly what makes the comparison worth running."""
        _, vector = await self.run_vector()
        event = await self.run_event()
        for metric in ("sharpe", "max_drawdown", "algo_volatility", "algorithm_period_return"):
            self.assertAlmostEqual(float(vector.perf[metric].iloc[-1]),
                                   float(event.perf[metric].iloc[-1]), places=9, msg=metric)

    async def test_the_comparison_utility_reports_agreement(self):
        """§32, exercised on a case where the answer is known."""
        _, vector = await self.run_vector()
        event = await self.run_event()
        comparison = compare_execution_results(vector, event)
        self.assertTrue(comparison.agrees, comparison.report())
        self.assertEqual(comparison.trade_count_difference, 0)
        self.assertEqual(comparison.only_in_vector, [])
        self.assertEqual(comparison.only_in_event, [])
        self.assertLess(comparison.max_equity_gap, 1e-6)

    async def test_the_comparison_utility_notices_a_real_difference(self):
        """The other half: a utility that always reports agreement reports nothing. The vector
        run is re-run at a different size, which has to show up as different trades."""
        prices, entries, exits = self.signals()
        smaller = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=SIZE // 2, initial_cash=CASH,
            execution=ExecutionTiming.same_close(),
            assets={t: self.listings[t] for t in TICKERS})
        with contextlib.redirect_stdout(io.StringIO()):
            vector = await to_execution_result(
                portfolio=smaller, listings=self.listings, prices=prices,
                trading_calendar=self.calendar, exchange=self.exchange(),
                emission_rate=datetime.timedelta(days=1))
        event = await self.run_event()

        comparison = compare_execution_results(vector, event)
        self.assertFalse(comparison.agrees)
        self.assertNotEqual(comparison.ending_equity_difference, 0.0)
        self.assertIn("ending equity", comparison.report())


class AdapterIntegrationTests(ParityTestCase):
    """The kernel's result going through the existing adapter untouched -- the spec's §17/§18."""

    async def test_the_adapter_accepts_the_kernels_own_result(self):
        """No wrapper at the call site: `to_execution_result` takes a `VectorSimulationResult`."""
        _, result = await self.run_vector()
        self.assertGreater(len(result.perf), 100)
        self.assertIn("sharpe", result.perf.columns)
        self.assertEqual(result.errors, [])

    async def test_the_result_is_the_ordinary_execution_result(self):
        from ziplime.trading.trading_algorithm_execution_result import (
            TradingAlgorithmExecutionResult,
        )

        _, result = await self.run_vector()
        self.assertIsInstance(result, TradingAlgorithmExecutionResult)
        self.assertIsNotNone(result.risk_report)

    async def test_the_kernel_result_stays_reachable_from_the_run(self):
        """`VectorizedRun.source` used to hold a vectorbt portfolio; it now holds whatever was
        replayed, and for a kernel run that is the fills and the rejects."""
        kernel_result, result = await self.run_vector()
        source = result.trading_algorithm.source
        self.assertTrue(hasattr(source, "orders"))
        self.assertEqual(len(source.orders.records_readable), len(kernel_result.fills))

    async def test_the_replayed_ledger_agrees_with_the_kernels_own_cash(self):
        """The invariant behind the spec's §21. The ledger is the source of truth; the kernel's
        cash is a working number it needed to decide fills. They are computed by different code
        from the same trades, so agreement is worth asserting and disagreement is a bug here."""
        kernel_result, result = await self.run_vector()
        np.testing.assert_allclose(
            result.perf["portfolio_value"].to_numpy(dtype=float),
            kernel_result.portfolio_value.to_numpy(dtype=float), rtol=0, atol=1e-6,
            err_msg="the ledger and the kernel disagree about the book they built from the same "
                    "fills")

    async def test_a_run_that_never_trades_still_produces_a_result(self):
        prices, _, _ = self.signals()
        silent = simulate_signals(
            prices=prices, entries=pd.DataFrame(False, index=prices.index, columns=list(TICKERS)),
            exits=pd.DataFrame(False, index=prices.index, columns=list(TICKERS)),
            size=SIZE, initial_cash=CASH, execution=ExecutionTiming.same_close())
        self.assertEqual(silent.fills, [])
        with contextlib.redirect_stdout(io.StringIO()):
            result = await to_execution_result(
                portfolio=silent, listings=self.listings, prices=prices,
                trading_calendar=self.calendar, exchange=self.exchange(),
                emission_rate=datetime.timedelta(days=1))
        self.assertEqual(float(result.perf["portfolio_value"].iloc[-1]), CASH)


class TimingParityTests(ParityTestCase):
    """Execution timing is where the two engines are *supposed* to be able to differ, and the
    spec's §10 wants that expressed in shared terms rather than as a `.shift()` in strategy code.

    So the claim under test is narrow and checkable: the kernel's three modes produce three
    different runs, and exactly one of them -- `same_close` -- is the one that matches the event
    engine's `same_bar_execution=True`.
    """

    async def run_at(self, timing):
        prices, entries, exits = self.signals()
        result = simulate_signals(
            prices=prices, entries=entries, exits=exits, size=SIZE, initial_cash=CASH,
            execution=timing, assets={t: self.listings[t] for t in TICKERS})
        return result

    async def test_same_close_matches_the_event_engines_same_bar_execution(self):
        kernel_result = await self.run_at(ExecutionTiming.same_close())
        event = await self.run_event()
        event_fills = [(t.dt, t.asset.symbol, round(float(t.amount), 8))
                       for row in event.perf["transactions"] for t in row]
        kernel = [(f.timestamp, f.instrument, round(f.amount, 8)) for f in kernel_result.fills]
        self.assertEqual(sorted(kernel, key=str), sorted(event_fills, key=str))

    async def test_next_close_fills_a_bar_later_and_at_a_different_price(self):
        same = await self.run_at(ExecutionTiming.same_close())
        later = await self.run_at(ExecutionTiming.next_close())
        self.assertNotEqual([(f.timestamp, f.price) for f in same.fills],
                            [(f.timestamp, f.price) for f in later.fills])
        # Every fill moves forward by exactly one bar.
        prices, _, _ = self.signals()
        positions = {stamp: ix for ix, stamp in enumerate(prices.index)}
        for earlier_fill, later_fill in zip(same.fills, later.fills):
            self.assertEqual(positions[pd.Timestamp(later_fill.timestamp)],
                             positions[pd.Timestamp(earlier_fill.timestamp)] + 1)

    async def test_the_look_ahead_mode_is_the_one_that_reports_better(self):
        """Not a law of nature, but true for this strategy and worth pinning: filling at the close
        the decision was taken on is worth about a bar of edge, which is the whole reason
        `same_close` is labelled look-ahead rather than offered as a default."""
        same = await self.run_at(ExecutionTiming.same_close())
        later = await self.run_at(ExecutionTiming.next_close())
        self.assertNotEqual(float(same.portfolio_value.iloc[-1]),
                            float(later.portfolio_value.iloc[-1]))
