"""Replaying a third-party engine's portfolio through ziplime's ledger.

Everything here needs vectorbt, which is why it lives beside the differential suite rather than
in the main test directory: `tests/vectorized/reference/vectorbt/` is the one place `pip uninstall vectorbt`
is allowed to silence.

What it covers is not vectorbt, though -- it is the adapter's ability to take fills decided
*somewhere else* and report them in ziplime's own terms. That capability is what makes a
cross-engine comparison mean anything: run two engines on the same signals, replay both through
the same ledger and the same metric set, and the only thing left to differ is the trading. The
kernel's own path through the same adapter is covered by
`tests/test_vector_kernel_parity.py::AdapterIntegrationTests`, which needs nothing installed.
"""
import datetime
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

try:
    import vectorbt as vbt
except ImportError:  # pragma: no cover - exercised only without the extra
    vbt = None

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.constants.data_type import DataType
from ziplime.core.ingest_data import get_asset_service
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.exchanges.simulation_exchange import SimulationExchange
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar

# Four levels up now: tests/vectorized/reference/vectorbt/ -> tests/vectorized/reference/ ->
# tests/vectorized/ -> tests/ -> the repository.
PROJECT_ROOT = Path(__file__).resolve().parents[4]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"

CALENDAR = "XNYS"
START, END = datetime.date(2024, 1, 3), datetime.date(2024, 3, 27)
CASH = 100_000.0
TICKERS = ("JNJ", "KO")

#: The same decisions the event-driven fixture places. Mirrored in
#: tests/fixtures/vectorized_equivalent.py, deliberately as literals on both sides: deriving them
#: once and sharing them would let a bug in the derivation cancel itself out.
SCHEDULE = {
    datetime.date(2024, 1, 10): {"JNJ": 100},
    datetime.date(2024, 1, 24): {"KO": 200},
    datetime.date(2024, 2, 14): {"JNJ": -100},
    datetime.date(2024, 3, 6): {"KO": -200},
}

requires_vectorbt = unittest.skipIf(vbt is None, "vectorbt is not installed")


def prices(index: pd.DatetimeIndex) -> pd.DataFrame:
    """A deterministic price path. Not random: a test that fails only on some seeds is not a test."""
    steps = np.arange(len(index))
    return pd.DataFrame({
        "JNJ": 100.0 + np.sin(steps / 7.0) * 6.0 + steps * 0.05,
        "KO": 50.0 + np.cos(steps / 5.0) * 3.0 - steps * 0.02,
    }, index=index)


class VectorizedTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-vbt-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(START, END)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.prices = prices(self.index)
        self.listings = {}
        for ticker in TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def exchange(self, cash: float = CASH) -> SimulationExchange:
        return SimulationExchange(
            name="VBT", country_code="US", trading_calendar=self.calendar, clock=None,
            cash_balance=cash, equity_slippage=NoSlippage(), future_slippage=NoSlippage(),
            equity_commission=NoCommission(), future_commission=NoCommission(),
            account_id="vectorized_account", is_default=True)

    def scheduled_portfolio(self, fees: float = 0.0):
        """A vectorbt portfolio holding exactly :data:`SCHEDULE`, via explicit order sizes."""
        size = pd.DataFrame(np.nan, index=self.index, columns=list(TICKERS))
        for session, orders in SCHEDULE.items():
            stamp = next(s for s in self.index if s.date() == session)
            for ticker, amount in orders.items():
                size.loc[stamp, ticker] = amount
        return vbt.Portfolio.from_orders(
            self.prices, size=size, size_type="amount", init_cash=CASH, freq="1D",
            fees=fees, group_by=True, cash_sharing=True)

    async def convert(self, portfolio, **kwargs):
        from ziplime.vectorized import to_execution_result

        return await to_execution_result(
            portfolio=portfolio, listings=self.listings, prices=self.prices,
            trading_calendar=self.calendar, exchange=self.exchange(),
            emission_rate=datetime.timedelta(days=1), **kwargs)


@requires_vectorbt
class FidelityTests(VectorizedTestCase):
    """The replayed book has to hold what vectorbt says it holds."""

    async def test_the_portfolio_value_matches_bar_for_bar(self):
        portfolio = self.scheduled_portfolio()
        result = await self.convert(portfolio)
        replayed = result.perf["portfolio_value"].to_numpy(dtype=float)
        reported = np.asarray(portfolio.value(), dtype=float)
        np.testing.assert_allclose(replayed, reported, rtol=0, atol=1e-6)

    async def test_the_cash_balance_matches_bar_for_bar(self):
        portfolio = self.scheduled_portfolio()
        result = await self.convert(portfolio)
        np.testing.assert_allclose(result.perf["ending_cash"].to_numpy(dtype=float),
                                   np.asarray(portfolio.cash(), dtype=float), rtol=0, atol=1e-6)

    async def test_the_daily_returns_match(self):
        """The series every risk metric is computed from. It was wrong at first: the replay did
        not open each session on the ledger, so `todays_returns` carried the whole run's return
        and every daily figure -- Sharpe, volatility, drawdown -- was computed on cumulative
        returns while the portfolio value still reconciled perfectly."""
        portfolio = self.scheduled_portfolio()
        result = await self.convert(portfolio)
        np.testing.assert_allclose(result.perf["returns"].to_numpy(dtype=float),
                                   np.asarray(portfolio.returns(), dtype=float),
                                   rtol=0, atol=1e-12)

    async def test_every_order_becomes_exactly_one_transaction(self):
        portfolio = self.scheduled_portfolio()
        result = await self.convert(portfolio)
        replayed = [t for row in result.perf["transactions"] for t in row]
        self.assertEqual(len(replayed), len(portfolio.orders.records_readable))
        self.assertEqual(len(replayed), sum(len(o) for o in SCHEDULE.values()))

    async def test_fees_are_carried_rather_than_modelled_again(self):
        """vectorbt applies fees inside its own fills. Charging them again here would show up as
        a reconciliation failure rather than as a slightly worse return."""
        result = await self.convert(self.scheduled_portfolio(fees=0.001))
        self.assertGreater(len(result.perf), 0)

    async def test_the_result_is_the_ordinary_execution_result(self):
        """Everything downstream -- the CLI report, the example harnesses -- takes this type."""
        from ziplime.trading.trading_algorithm_execution_result import (
            TradingAlgorithmExecutionResult,
        )

        result = await self.convert(self.scheduled_portfolio())
        self.assertIsInstance(result, TradingAlgorithmExecutionResult)
        self.assertEqual(result.errors, [])
        for column in ("portfolio_value", "returns", "sharpe", "max_drawdown", "transactions",
                       "ending_cash", "positions", "gross_leverage"):
            self.assertIn(column, result.perf.columns)

    async def test_the_vectorbt_portfolio_stays_reachable(self):
        portfolio = self.scheduled_portfolio()
        result = await self.convert(portfolio)
        self.assertIs(result.trading_algorithm.source, portfolio)
        self.assertAlmostEqual(result.trading_algorithm.portfolio.portfolio_value,
                               float(portfolio.value().iloc[-1]), places=6)


@requires_vectorbt
class RefusalTests(VectorizedTestCase):
    """What the adapter refuses, and why silence would be worse."""

    async def test_an_unmapped_column_is_refused(self):
        """Skipping it would produce a book short some trades, that still looks like a book."""
        portfolio = self.scheduled_portfolio()
        with self.assertRaises(KeyError) as raised:
            from ziplime.vectorized import to_execution_result
            await to_execution_result(
                portfolio=portfolio, listings={"JNJ": self.listings["JNJ"]}, prices=self.prices,
                trading_calendar=self.calendar, exchange=self.exchange())
        self.assertIn("KO", str(raised.exception))

    async def test_a_divergence_is_refused_rather_than_reported(self):
        """Starting the ledger with different capital than vectorbt used makes every bar disagree.
        A hybrid whose halves disagree silently is worse than one half alone."""
        from ziplime.vectorized import ReconciliationError, to_execution_result

        with self.assertRaises(ReconciliationError) as raised:
            await to_execution_result(
                portfolio=self.scheduled_portfolio(), listings=self.listings, prices=self.prices,
                trading_calendar=self.calendar, exchange=self.exchange(cash=50_000.0))
        # "the source", not "vectorbt": the same replay now runs the kernel's fills far more
        # often than a third-party portfolio's, and a reconciliation failure there would have
        # named a library that was not involved.
        self.assertIn("the source reports", str(raised.exception))

    async def test_the_check_can_be_turned_off_for_debugging(self):
        from ziplime.vectorized import to_execution_result

        result = await to_execution_result(
            portfolio=self.scheduled_portfolio(), listings=self.listings, prices=self.prices,
            trading_calendar=self.calendar, exchange=self.exchange(cash=50_000.0),
            reconcile=False)
        self.assertGreater(len(result.perf), 0)

    async def test_a_parameter_sweep_is_refused_with_an_explanation(self):
        """`from_signals` over a grid produces one value series per combination. Reconciling needs
        one book, and saying which is the caller's decision."""
        from ziplime.vectorized import to_execution_result

        windows = [3, 5]
        fast = vbt.MA.run(self.prices, window=windows)
        slow = vbt.MA.run(self.prices, window=[10, 20])
        sweep = vbt.Portfolio.from_signals(
            self.prices, fast.ma_above(slow), fast.ma_below(slow), init_cash=CASH, freq="1D")
        with self.assertRaises(ValueError) as raised:
            await to_execution_result(
                portfolio=sweep, listings=self.listings, prices=self.prices,
                trading_calendar=self.calendar, exchange=self.exchange())
        self.assertIn("more than one", str(raised.exception))


class SessionBoundaryTests(unittest.TestCase):
    """Bars grouped into sessions -- the thing an intraday replay is built on.

    Its first version returned sets. Sets of small integers iterate in ascending order, so every
    daily test and every short intraday run agreed with it, and a run long enough for the positions
    to spread out did not: the session closes came back shuffled, the benchmark was asked for a
    range running backwards in time, and the failure surfaced three modules away as a metric
    subscripting a None. Ordered lists, and a test that says so.
    """

    def minute_index(self, sessions: int) -> pd.DatetimeIndex:
        from ziplime.data.services.bar_alignment import clock_minutes

        calendar = get_calendar(CALENDAR)
        days = calendar.sessions_in_range(datetime.date(2024, 1, 3), datetime.date(2024, 12, 31))
        chosen = days[:sessions]
        grid = clock_minutes(calendar, chosen[0].date(), chosen[-1].date(),
                             datetime.timedelta(minutes=1))
        return pd.DatetimeIndex(grid.to_pandas())

    def test_one_boundary_pair_per_session(self):
        from ziplime.vectorized.adapter import _session_boundaries

        index = self.minute_index(4)
        opening, closing = _session_boundaries(index)
        self.assertEqual(len(opening), 4)
        self.assertEqual(len(closing), 4)

    def test_the_boundaries_come_back_in_order(self):
        """The bug. Large enough that set iteration would not have saved it."""
        from ziplime.vectorized.adapter import _session_boundaries

        index = self.minute_index(60)
        opening, closing = _session_boundaries(index)
        self.assertEqual(opening, sorted(opening))
        self.assertEqual(closing, sorted(closing))
        sessions = [index[ix].date() for ix in closing]
        self.assertEqual(sessions, sorted(sessions), "sessions reached the metrics out of order")

    def test_each_close_is_the_last_bar_of_its_session(self):
        from ziplime.vectorized.adapter import _session_boundaries

        index = self.minute_index(5)
        _, closing = _session_boundaries(index)
        for ix in closing:
            self.assertTrue(ix + 1 == len(index) or index[ix + 1].date() != index[ix].date())

    def test_at_a_daily_rate_every_bar_is_its_own_session(self):
        """What keeps the daily path exactly as it was."""
        from ziplime.vectorized.adapter import _session_boundaries

        calendar = get_calendar(CALENDAR)
        days = calendar.sessions_in_range(datetime.date(2024, 1, 3), datetime.date(2024, 3, 27))
        index = pd.DatetimeIndex(calendar.schedule.loc[days, "close"].dt.tz_convert(calendar.tz))
        opening, closing = _session_boundaries(index)
        self.assertEqual(opening, list(range(len(index))))
        self.assertEqual(closing, list(range(len(index))))


@requires_vectorbt
class MinuteBarTests(VectorizedTestCase):
    """A replay at a minute rate, where a bar and a session are not the same thing.

    The first version treated every bar as a session: it handed the ledger a returns array 390
    times too long and crashed inside empyrical on shapes that could not align. Nothing caught it
    because every test ran daily bars, where the distinction does not exist.
    """

    SESSIONS = 5

    async def asyncSetUp(self):
        await super().asyncSetUp()
        from ziplime.data.services.bar_alignment import clock_minutes

        days = self.calendar.sessions_in_range(datetime.date(2024, 1, 3),
                                               datetime.date(2024, 12, 31))[:self.SESSIONS]
        self.days = [d.date() for d in days]
        grid = clock_minutes(self.calendar, self.days[0], self.days[-1],
                             datetime.timedelta(minutes=1))
        self.minutes = pd.DatetimeIndex(grid.to_pandas())
        steps = np.arange(len(self.minutes))
        self.minute_prices = pd.DataFrame(
            {"JNJ": 150.0 + np.sin(steps / 120.0) * 4.0 + np.sin(steps / 17.0) * 0.6},
            index=self.minutes)

    def minute_portfolio(self):
        fast = self.minute_prices.rolling(20).mean()
        slow = self.minute_prices.rolling(60).mean()
        return vbt.Portfolio.from_signals(
            self.minute_prices, (fast > slow) & (fast.shift() <= slow.shift()),
            (fast < slow) & (fast.shift() >= slow.shift()), init_cash=CASH, freq="1min",
            size=10, size_type="amount", group_by=True, cash_sharing=True)

    async def convert_minutes(self, **kwargs):
        from ziplime.vectorized import to_execution_result

        return await to_execution_result(
            portfolio=self.minute_portfolio(), listings={"JNJ": self.listings["JNJ"]},
            prices=self.minute_prices, trading_calendar=self.calendar,
            exchange=self.exchange(), emission_rate=datetime.timedelta(minutes=1), **kwargs)

    async def test_the_result_has_one_row_per_session_not_per_bar(self):
        result = await self.convert_minutes()
        self.assertEqual(len(result.perf), self.SESSIONS)
        self.assertGreater(len(self.minutes), 1000, "the two would be the same on a short index")

    async def test_the_rows_are_stamped_at_the_session_closes(self):
        result = await self.convert_minutes()
        self.assertEqual([stamp.date() for stamp in result.perf.index], self.days)

    async def test_the_value_matches_vectorbt_at_each_close(self):
        """Reconciliation runs on every bar; this checks the reported table as well."""
        portfolio = self.minute_portfolio()
        result = await self.convert_minutes()
        reported = portfolio.value()
        closes = [reported.index.get_loc(stamp) for stamp in result.perf.index]
        np.testing.assert_allclose(result.perf["portfolio_value"].to_numpy(dtype=float),
                                   np.asarray(reported, dtype=float)[closes],
                                   rtol=0, atol=1e-6)

    async def test_the_trades_all_arrive(self):
        portfolio = self.minute_portfolio()
        result = await self.convert_minutes()
        replayed = [t for row in result.perf["transactions"] for t in row]
        self.assertGreater(len(replayed), 4, "a portfolio that never traded proves nothing")
        self.assertEqual(len(replayed), len(portfolio.orders.records_readable))

    async def test_reconciliation_still_fires_on_intraday_bars(self):
        """The replay revalues the book only where the result is read, which is most of its speed.
        The check that the two engines still agree has to survive that."""
        from ziplime.vectorized import to_execution_result
        from ziplime.vectorized.adapter import ReconciliationError

        with self.assertRaises(ReconciliationError):
            await to_execution_result(
                portfolio=self.minute_portfolio(), listings={"JNJ": self.listings["JNJ"]},
                prices=self.minute_prices, trading_calendar=self.calendar,
                # Starting somewhere other than the portfolio's init_cash puts the two sides apart
                # on every bar, which is the cheapest way to prove the comparison is happening.
                exchange=self.exchange(cash=CASH * 2),
                emission_rate=datetime.timedelta(minutes=1))

    async def test_trades_are_reconciled_at_the_bar_they_priced(self):
        """Session closes alone would let a bad fill hide until the end of the day.

        Proved by poisoning vectorbt's reported value at one mid-session trade bar and requiring
        the replay to notice. Skip that bar and the divergence is absorbed by the next close,
        where the two sides agree again -- which is exactly the hole this guards.
        """
        from ziplime.vectorized import adapter
        from ziplime.vectorized.adapter import ReconciliationError, _session_boundaries

        portfolio = self.minute_portfolio()
        _, closing = _session_boundaries(self.minutes)
        close_stamps = {self.minutes[ix] for ix in closing}
        off_close = [t for t in portfolio.orders.records_readable["Timestamp"]
                     if t not in close_stamps]
        self.assertTrue(off_close, "every trade landed on a session close; this proves nothing")
        poisoned_bar = self.minutes.get_loc(off_close[0])

        original = adapter._value_at
        adapter._value_at = lambda series, ix: (
            original(series, ix) + 1_000.0 if ix == poisoned_bar else original(series, ix))
        try:
            with self.assertRaises(ReconciliationError):
                await self.convert_minutes()
        finally:
            adapter._value_at = original

    async def test_the_metrics_are_computed_on_sessions(self):
        """A Sharpe ratio annualised from 1,950 'sessions' would be off by a factor of 20."""
        result = await self.convert_minutes()
        self.assertTrue(np.isfinite(float(result.perf["sharpe"].iloc[-1])))
        self.assertEqual(len(result.perf["returns"]), self.SESSIONS)


@requires_vectorbt
class InstrumentTests(VectorizedTestCase):
    """A vectorbt portfolio cannot stand for a contract, and says so before it replays one.

    The reconciliation would catch a futures replay anyway -- the value comes out wrong by exactly
    the multiplier -- but it would report it as a mismatch whose three listed causes are all the
    wrong ones. A multiplier is knowable up front, so it is refused up front.
    """

    def futures_listing(self, multiplier: float = 1000.0):
        from tests.futures_fixtures import make_future

        return make_future(sid=90_001, symbol="CLZ23", multiplier=multiplier)

    async def convert_listing(self, listing):
        from ziplime.vectorized import to_execution_result

        portfolio = self.scheduled_portfolio()
        listings = dict(self.listings)
        listings["JNJ"] = listing
        return await to_execution_result(
            portfolio=portfolio, listings=listings, prices=self.prices,
            trading_calendar=self.calendar, exchange=self.exchange(),
            emission_rate=datetime.timedelta(days=1))

    async def test_a_futures_contract_is_refused(self):
        with self.assertRaises(ValueError) as raised:
            await self.convert_listing(self.futures_listing())
        message = str(raised.exception)
        self.assertIn("CLZ23", message)
        self.assertIn("multiplier 1000", message, "the refusal names the number that is wrong")
        self.assertIn("margin", message)

    async def test_the_refusal_points_at_the_event_driven_path(self):
        with self.assertRaises(ValueError) as raised:
            await self.convert_listing(self.futures_listing())
        self.assertIn("vectorized.signals", str(raised.exception))

    async def test_a_futures_contract_with_a_unit_multiplier_is_still_refused(self):
        """The multiplier is only half of it -- margin makes the cash line wrong on its own."""
        with self.assertRaises(ValueError) as raised:
            await self.convert_listing(self.futures_listing(multiplier=1.0))
        self.assertIn("margined", str(raised.exception))

    async def test_equities_are_not_refused(self):
        result = await self.convert(self.scheduled_portfolio())
        self.assertGreater(len(result.perf), 0)

    async def test_it_is_refused_before_anything_is_replayed(self):
        """Not as a reconciliation failure several hundred bars in, where the message would name
        starting cash and price series and send the author somewhere else entirely."""
        from ziplime.vectorized.adapter import ReconciliationError

        with self.assertRaises(ValueError) as raised:
            await self.convert_listing(self.futures_listing())
        self.assertNotIsInstance(raised.exception, ReconciliationError)


@requires_vectorbt
class CostModelTests(VectorizedTestCase):
    """Costs on this path are vectorbt's, and an exchange that says otherwise is refused.

    ziplime's commission and slippage models live in the blotter, and no order on this path goes
    through it. An exchange configured with them would produce a backtest that paid nothing while
    its author believed it had paid -- and unpaid costs do not surface as an error, they surface
    as a better return, which is the failure mode worth the most noise.
    """

    def costed_exchange(self, commission=None, slippage=None):
        from ziplime.finance.commission.per_share import PerShare

        return SimulationExchange(
            name="VBT", country_code="US", trading_calendar=self.calendar, clock=None,
            cash_balance=CASH, equity_slippage=slippage or NoSlippage(),
            future_slippage=NoSlippage(),
            equity_commission=commission or PerShare(cost=0.005, min_trade_cost=1.0),
            future_commission=NoCommission(), account_id="vectorized_account", is_default=True)

    async def convert_with(self, exchange, **kwargs):
        from ziplime.vectorized import to_execution_result

        return await to_execution_result(
            portfolio=self.scheduled_portfolio(), listings=self.listings, prices=self.prices,
            trading_calendar=self.calendar, exchange=exchange,
            emission_rate=datetime.timedelta(days=1), **kwargs)

    async def test_a_commission_model_that_could_not_run_is_refused(self):
        with self.assertRaises(ValueError) as raised:
            await self.convert_with(self.costed_exchange())
        message = str(raised.exception)
        self.assertIn("PerShare", message, "the refusal names the model that would be ignored")
        self.assertIn("simulate_signals", message, "and where the costs belong instead")

    async def test_a_slippage_model_that_could_not_run_is_refused(self):
        from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage

        with self.assertRaises(ValueError) as raised:
            await self.convert_with(self.costed_exchange(
                commission=NoCommission(), slippage=FixedBasisPointsSlippage(basis_points=5)))
        self.assertIn("FixedBasisPointsSlippage", str(raised.exception))

    async def test_the_refusal_points_at_the_event_driven_path(self):
        """Someone who wants ziplime's cost models wants the other half of the hybrid."""
        with self.assertRaises(ValueError) as raised:
            await self.convert_with(self.costed_exchange())
        self.assertIn("vectorized.signals", str(raised.exception))

    async def test_it_can_be_overridden_deliberately(self):
        result = await self.convert_with(self.costed_exchange(), ignore_exchange_costs=True)
        self.assertGreater(len(result.perf), 0)

    async def test_a_costless_exchange_is_not_refused(self):
        result = await self.convert_with(self.exchange())
        self.assertGreater(len(result.perf), 0)

    async def test_vectorbt_fees_are_booked_into_the_ledger(self):
        """The costs that *do* apply here: vectorbt's own, carried through rather than recomputed."""
        free = await self.convert(self.scheduled_portfolio())
        costed = await self.convert(self.scheduled_portfolio(fees=0.001))
        self.assertLess(float(costed.perf["portfolio_value"].iloc[-1]),
                        float(free.perf["portfolio_value"].iloc[-1]),
                        "vectorbt charged fees and the replayed book did not notice")
        charged = sum(float(t.commission) for row in costed.perf["transactions"] for t in row)
        self.assertGreater(charged, 0.0)


@requires_vectorbt
class EquivalenceTests(VectorizedTestCase):
    """The same trades through both engines produce the same table.

    This is the claim the whole design exists for. If it fails, the hybrid is two backtesters that
    happen to share a repository.
    """

    async def event_driven(self):
        """Run the fixture strategy through the ordinary engine over the same prices."""
        from ziplime.core.run_simulation import run_simulation

        rows = [{"date": stamp, "sid": self.listings[ticker].sid, "symbol": ticker, "mic": "XNYS",
                 "open": price, "high": price, "low": price, "close": price, "price": price,
                 "volume": 1e9}
                for ticker in TICKERS
                for stamp, price in self.prices[ticker].items()]
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        bundle = DataBundle(
            name="vbt-equivalence", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)})

        return await run_simulation(
            start_date=datetime.datetime.combine(START, datetime.time.min,
                                                 tzinfo=self.calendar.tz),
            end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
            trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
            total_cash=CASH, market_data_source=bundle, custom_data_sources=[],
            algorithm_file=str(FIXTURES / "vectorized_equivalent.py"), stop_on_error=True,
            asset_service=self.asset_service, benchmark_asset_symbol=None, benchmark_returns=None,
            equity_commission=NoCommission(), equity_slippage=NoSlippage(),
            max_leverage=10.0,
            # vectorbt fills a signal at the close of the bar that produced it, so the event-driven
            # run has to do the same or the two are trading different prices, not comparing engines.
            same_bar_execution=True, price_used_in_order_execution="close", print_algo=False)

    async def test_both_engines_end_with_the_same_money(self):
        vectorised = await self.convert(self.scheduled_portfolio())
        event_driven = await self.event_driven()
        self.assertAlmostEqual(float(vectorised.perf["portfolio_value"].iloc[-1]),
                               float(event_driven.perf["portfolio_value"].iloc[-1]), places=6)

    async def test_both_engines_agree_bar_for_bar(self):
        vectorised = await self.convert(self.scheduled_portfolio())
        event_driven = await self.event_driven()
        for column in ("portfolio_value", "ending_cash", "ending_exposure", "returns"):
            np.testing.assert_allclose(
                vectorised.perf[column].to_numpy(dtype=float),
                event_driven.perf[column].to_numpy(dtype=float),
                rtol=0, atol=1e-6, err_msg=f"{column} differs between the two engines")

    async def test_the_risk_metrics_agree(self):
        """The point of replaying rather than translating: one Sharpe ratio, not two conventions."""
        vectorised = await self.convert(self.scheduled_portfolio())
        event_driven = await self.event_driven()
        for column in ("sharpe", "max_drawdown", "algo_volatility", "algorithm_period_return"):
            self.assertAlmostEqual(
                float(vectorised.perf[column].iloc[-1]),
                float(event_driven.perf[column].iloc[-1]), places=6, msg=column)

    async def test_both_engines_book_the_same_trades(self):
        vectorised = await self.convert(self.scheduled_portfolio())
        event_driven = await self.event_driven()

        def signature(result):
            return sorted((t.dt.date(), t.asset.symbol, t.amount, round(t.price, 6))
                          for row in result.perf["transactions"] for t in row)

        self.assertEqual(signature(vectorised), signature(event_driven))


if __name__ == "__main__":
    unittest.main(verbosity=2)
