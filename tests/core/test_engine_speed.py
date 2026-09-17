"""What a bar costs, across bar rates and asset classes, and what vectorising the signals buys.

Two questions, and they are not the same question.

**What does a bar cost?** A run's wall time is bars times cost-per-bar, and the two move
independently: going from daily to minute bars multiplies the bar count by about 390 and should
leave the cost of each one alone. When it does not -- when per-bar cost climbs with the length of
the run -- the engine has gone quadratic somewhere, and the symptom is a run that is fine in a
test and hopeless over ten years. :class:`ScalingTests` is the guard, and it is the assertion in
this file most worth keeping.

**What does vectorising buy?** ziplime has no `engine="vector"` switch, and this file does not
invent one. What it has is
:mod:`ziplime.vectorized.signals`: a strategy computes its indicators over the whole history in
one pass before the first bar, then trades them bar by bar through the ordinary blotter. The
execution is identical -- same fills, same slippage, same commissions -- and only the arithmetic
moves. So the comparison here is a strategy against *itself*, written both ways, which is the
only comparison where a time difference means anything.

(The other vectorised path, :func:`ziplime.vectorized.adapter.to_execution_result`, replays a
vectorbt portfolio. That one is not timed here: vectorbt has already decided the fills, so it is
not running the same backtest and a speed ratio against it would be measuring the absence of an
execution model.)

**The numbers are printed, not asserted.** Wall-clock thresholds in a test suite fail on a busy
laptop and pass on a fast one having measured nothing, so what is asserted is only what survives
a machine being slow: that the two versions of a strategy agree on the orders they place, that
per-bar cost does not grow with the length of the run, and that the vectorised path wins on the
strategies whose signals dominate. Run with `-s` to see the table.
"""
import contextlib
import datetime
import io
import shutil
import tempfile
import time
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
from ziplime.data.services.bar_alignment import clock_minutes
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"
CALENDAR = "XNYS"
TICKERS = ("JNJ", "KO")
CASH = 100_000.0

#: Every rate the engine is asked to run at. `1w` is here because it is the one that used to
#: crash -- see `WeeklyRateTests`.
RATES = {
    "1m": datetime.timedelta(minutes=1),
    "15m": datetime.timedelta(minutes=15),
    "1h": datetime.timedelta(hours=1),
    "1d": datetime.timedelta(days=1),
    "1w": datetime.timedelta(weeks=1),
}

#: Runs per measurement. The minimum of a few is a far better estimate of the work done than the
#: mean of a few: it is the run that was interrupted least, and nothing here gets faster by luck.
REPEATS = 3

#: Collected across the whole file and printed once at the end, because one row on its own says
#: nothing -- the comparison between rows is the measurement.
TABLE: list[tuple] = []


def record(group: str, case: str, bars: int, seconds: float, note: str = "") -> None:
    TABLE.append((group, case, bars, seconds, bars / seconds if seconds else 0.0, note))


def tearDownModule():  # noqa: N802 - unittest's spelling
    if not TABLE:
        return
    width = max(len(f"{g}/{c}") for g, c, *_ in TABLE)
    print(f"\n\n{'case':<{width}}  {'bars':>7}  {'seconds':>8}  {'bars/s':>8}  note")
    print("-" * (width + 42))
    for group, case, bars, seconds, rate, note in TABLE:
        print(f"{group + '/' + case:<{width}}  {bars:>7}  {seconds:>8.3f}  {rate:>8.0f}  {note}")


def price_frame(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Deterministic paths that cross often enough for a signal to trade, and diverge enough for
    a cross-sectional rank to change its mind."""
    steps = np.arange(len(index))
    return pd.DataFrame({
        "JNJ": 100.0 + np.sin(steps / 11.0) * 9.0 + np.sin(steps / 3.0) * 2.0 + steps * 0.002,
        "KO": 50.0 + np.cos(steps / 8.0) * 5.0 + np.cos(steps / 2.5) * 1.5 - steps * 0.001,
    }, index=index)


def build_bundle(name: str, frame: pd.DataFrame, sids: dict[str, int], calendar,
                 rate: datetime.timedelta, mic: str = "XNYS") -> DataBundle:
    rows = [{"date": stamp, "sid": sids[ticker], "symbol": ticker, "mic": mic,
             "open": price, "high": price, "low": price, "close": price, "price": price,
             "volume": 1e9}
            for ticker in frame.columns for stamp, price in frame[ticker].items()]
    data = pl.DataFrame(rows).sort(["sid", "date"])
    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
    return DataBundle(
        name=name, version="1", start_date=data["date"].min(), end_date=data["date"].max(),
        trading_calendar=calendar, frequency=rate, original_frequency=rate,
        data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
        sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                     for r in spans.iter_rows(named=True)})


def order_signatures(result) -> list[tuple]:
    """Every fill, in order, as the tuple that decides whether two runs did the same thing."""
    return [(t.asset.symbol, t.amount, round(float(t.price), 8))
            for row in result.perf["transactions"] for t in row]


class EquityHarness(unittest.IsolatedAsyncioTestCase):
    """A copy of the seeded asset database and an in-memory bundle. No network, no ingest."""

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-speed-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        self.listings = {}
        for ticker in TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)
        self.sids = {t: listing.sid for t, listing in self.listings.items()}

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    def window(self, sessions: int, rate: datetime.timedelta):
        """A bundle window one session wider than the run, so a warm-up has history to warm on."""
        days = self.calendar.sessions_in_range(datetime.date(2024, 1, 3), datetime.date(2024, 12, 20))
        days = days[:sessions + 1]
        bundle_start, start, end = days[0].date(), days[1].date(), days[-1].date()
        if rate < datetime.timedelta(days=1):
            index = pd.DatetimeIndex(clock_minutes(self.calendar, bundle_start, end, rate).to_pandas())
        else:
            in_range = self.calendar.sessions_in_range(bundle_start, end)
            index = pd.DatetimeIndex(
                self.calendar.schedule.loc[in_range, "close"].dt.tz_convert(self.calendar.tz))
        return start, end, index

    async def run_strategy(self, fixture: str, *, sessions: int = 60,
                           rate: datetime.timedelta = datetime.timedelta(days=1),
                           custom_data_sources=None):
        from ziplime.core.run_simulation import run_simulation

        start, end, index = self.window(sessions, rate)
        bundle = build_bundle("speed", price_frame(index), self.sids, self.calendar, rate)
        # The engine prints a warning banner per run; it is not what is being measured.
        with contextlib.redirect_stdout(io.StringIO()):
            return await run_simulation(
                start_date=datetime.datetime.combine(start, datetime.time.min,
                                                     tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(end, datetime.time.max,
                                                   tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=rate, total_cash=CASH,
                market_data_source=bundle, custom_data_sources=custom_data_sources or [],
                algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None, equity_commission=NoCommission(),
                equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False)

    async def time_strategy(self, fixture: str, **kwargs) -> tuple[float, object]:
        timings, result = [], None
        for _ in range(REPEATS):
            started = time.perf_counter()
            result = await self.run_strategy(fixture, **kwargs)
            timings.append(time.perf_counter() - started)
        return min(timings), result


#: The strategies this file compares, as (label, vectorised fixture, bar-by-bar fixture).
#: Each pair is the same strategy written twice, which is what makes a time difference meaningful.
PAIRS = [
    ("technical", "signals_vectorised.py", "signals_bar_by_bar.py"),
    ("rotation", "rotation_vectorised.py", "rotation_bar_by_bar.py"),
]


class VectorisedSignalsAgreeTests(EquityHarness):
    """Before any timing: the two versions have to be the same strategy.

    A speed comparison between a strategy and a subtly different strategy measures nothing, and
    the difference is easy to introduce -- `data.history` stops before the current bar while the
    signal panel's row *is* the current bar, so an off-by-one trades a different system without
    raising.
    """

    async def test_each_pair_places_identical_orders(self):
        for label, vectorised, bar_by_bar in PAIRS:
            with self.subTest(strategy=label):
                fast = await self.run_strategy(vectorised)
                slow = await self.run_strategy(bar_by_bar)
                placed = order_signatures(fast)
                self.assertGreater(len(placed), 2,
                                   f"{label} never traded, so agreement proves nothing")
                self.assertEqual(placed, order_signatures(slow),
                                 f"{label}: the two versions are not the same strategy")

    async def test_each_pair_reports_the_same_return(self):
        for label, vectorised, bar_by_bar in PAIRS:
            with self.subTest(strategy=label):
                fast = await self.run_strategy(vectorised)
                slow = await self.run_strategy(bar_by_bar)
                self.assertAlmostEqual(
                    float(fast.perf["algorithm_period_return"].iloc[-1]),
                    float(slow.perf["algorithm_period_return"].iloc[-1]), places=9,
                    msg=f"{label}: vectorising the signals changed the answer")


#: Sessions per rate for the vector-vs-event sweep, sized so each case lands in a comparable
#: number of bars rather than a comparable number of days.
SWEEP = {"1m": 4, "15m": 20, "1h": 40, "1d": 250, "1w": 250}

#: What counts as the vectorised path winning where it is supposed to. Measured at 2.2x-2.6x on
#: minute and 15-minute bars; asserted well below that so a loaded machine does not fail the
#: suite, but far enough above 1.0 to catch the mechanism regressing.
INTRADAY_SPEEDUP = 1.2

#: Daily runs have few enough bars that fixed per-run cost dominates and the measured margin is
#: only ~1.35x. Not enough to assert a win on; enough to assert the vectorised path is not paying
#: a penalty for machinery it is meant to be saving with.
MAXIMUM_PENALTY = 1.15


class VectorisedVersusBarByBarSpeedTests(EquityHarness):
    """The measurement the whole file exists for, swept across the rate axis.

    Sweeping the rate is what makes this informative rather than a single ratio. The vectorised
    path removes one `data.history` window per instrument per bar, so what it saves scales with
    the number of *bars*, not the number of days -- and the bar count is exactly what the emission
    rate decides. A day of minute bars is 390 of them; a day of daily bars is one. So the
    mechanism is worth little on a daily run and worth a great deal on an intraday one, which is
    the opposite of the intuition that a longer backtest benefits more.
    """

    async def test_the_advantage_grows_as_the_bars_get_finer(self):
        measured = {}
        for label, rate in RATES.items():
            with self.subTest(rate=label):
                sessions = SWEEP[label]
                fast_time, fast = await self.time_strategy(
                    "signals_vectorised.py", sessions=sessions, rate=rate)
                slow_time, slow = await self.time_strategy(
                    "signals_bar_by_bar.py", sessions=sessions, rate=rate)
                _, _, index = self.window(sessions, rate)
                speedup = slow_time / fast_time
                measured[label] = speedup
                record("vector-vs-event", f"technical {label}: vectorised", len(index), fast_time,
                       f"{speedup:.2f}x faster than bar-by-bar")
                record("vector-vs-event", f"technical {label}: bar-by-bar", len(index), slow_time,
                       "")
                self.assertLess(
                    fast_time, slow_time * MAXIMUM_PENALTY,
                    f"at {label} the vectorised path took {fast_time:.3f}s against the bar-by-bar "
                    f"{slow_time:.3f}s -- it is now costing more than it saves")

        for label in ("1m", "15m"):
            self.assertGreater(
                measured[label], INTRADAY_SPEEDUP,
                f"at {label} vectorising the signals was only {measured[label]:.2f}x faster. "
                f"Intraday is where this mechanism earns its keep -- one history window per "
                f"instrument per bar, and there are hundreds of bars a session.")

    async def test_a_rotation_agrees_and_is_no_slower(self):
        """A cross-sectional signal, where the bar-by-bar version has to read every name before it
        can rank any of them."""
        fast_time, fast = await self.time_strategy(
            "rotation_vectorised.py", sessions=20, rate=datetime.timedelta(minutes=15))
        slow_time, slow = await self.time_strategy(
            "rotation_bar_by_bar.py", sessions=20, rate=datetime.timedelta(minutes=15))
        record("vector-vs-event", "rotation 15m: vectorised", len(fast.perf), fast_time,
               f"{slow_time / fast_time:.2f}x faster than bar-by-bar")
        record("vector-vs-event", "rotation 15m: bar-by-bar", len(slow.perf), slow_time, "")
        self.assertLess(fast_time, slow_time * MAXIMUM_PENALTY)

    async def test_a_strategy_with_no_signal_is_not_slowed_by_the_machinery(self):
        """Buy-and-hold reads no history at all, so it is the control: whatever the vectorised
        path costs to set up, a strategy that does not use it must not pay for it."""
        seconds, result = await self.time_strategy("buy_two_and_hold.py")
        record("vector-vs-event", "buy-and-hold (no signals)", len(result.perf), seconds,
               "control: neither path")
        self.assertGreater(len(result.perf), 0)


class BarRateTests(EquityHarness):
    """Cost per bar across every rate the engine emits at.

    The bar counts are deliberately uneven -- three sessions of minutes is already more bars than
    a year of sessions -- so the column to read is bars/second, not seconds.
    """

    #: Sessions per rate, chosen so each case lands in the same order of magnitude of bars.
    SESSIONS = {"1m": 3, "15m": 6, "1h": 20, "1d": 120, "1w": 120}

    async def test_every_rate_runs_and_reports_its_throughput(self):
        for label, rate in RATES.items():
            with self.subTest(rate=label):
                sessions = self.SESSIONS[label]
                seconds, result = await self.time_strategy(
                    "signals_bar_by_bar.py", sessions=sessions, rate=rate)
                _, _, index = self.window(sessions, rate)
                record("bar-rate", label, len(index), seconds,
                       f"{len(result.perf)} rows in the performance table")
                self.assertGreater(len(result.perf), 0, f"{label} produced no rows")


class WeeklyRateTests(EquityHarness):
    """A weekly emission rate, which used to be unreachable.

    `SimulationClock` emits one bar per *session* for any rate of a day or more, but the benchmark
    series was bucketed by the raw emission rate -- 52 rows against the ledger's 245 -- and
    `AlphaBeta` sliced both by the same session index until the shorter ran out:

        ValueError: operands could not be broadcast together with shapes (26,1) () (25,1)

    A message that names neither the benchmark nor the rate, on a run that had been fine for its
    first 25 sessions. Fixed in `ziplime.utils.run_algo.benchmark_bucket`.
    """

    async def test_a_weekly_run_completes(self):
        result = await self.run_strategy(
            "signals_bar_by_bar.py", sessions=120, rate=datetime.timedelta(weeks=1))
        self.assertGreater(len(result.perf), 25,
                           "the run stopped around the 26th session, which is the old crash")

    async def test_a_rate_above_a_day_still_steps_session_by_session(self):
        """Worth pinning because it is surprising, and because it is what the fix has to match.

        A weekly rate does not produce weekly bars: the clock has one tick per session at any rate
        of a day or more. Weekly *bars* are a property of the bundle, not of the emission rate.
        """
        weekly = await self.run_strategy(
            "signals_bar_by_bar.py", sessions=60, rate=datetime.timedelta(weeks=1))
        daily = await self.run_strategy(
            "signals_bar_by_bar.py", sessions=60, rate=datetime.timedelta(days=1))
        self.assertEqual(len(weekly.perf), len(daily.perf))

    async def test_the_benchmark_bucket_never_exceeds_a_day(self):
        from ziplime.utils.run_algo import benchmark_bucket

        day = datetime.timedelta(days=1)
        self.assertEqual(benchmark_bucket(datetime.timedelta(minutes=1)),
                         datetime.timedelta(minutes=1))
        self.assertEqual(benchmark_bucket(day), day)
        self.assertEqual(benchmark_bucket(datetime.timedelta(weeks=1)), day)
        self.assertEqual(benchmark_bucket(datetime.timedelta(days=30)), day)


class ScalingTests(EquityHarness):
    """The assertion in this file most worth keeping.

    Wall time is bars times cost-per-bar. Doubling the bars should roughly double the time; if it
    more than quadruples it, cost-per-bar is growing with the length of the run and the engine has
    gone quadratic -- an unbounded history scan, a list that is appended to and re-read, a frame
    rebuilt from the whole run on every bar. That kind of regression passes every correctness test
    and makes long runs impossible, which is exactly the sort of thing only a timing test catches.
    """

    async def test_cost_per_bar_does_not_grow_with_the_length_of_the_run(self):
        short_time, short = await self.time_strategy("signals_bar_by_bar.py", sessions=40)
        long_time, long_run = await self.time_strategy("signals_bar_by_bar.py", sessions=160)

        short_bars, long_bars = len(short.perf), len(long_run.perf)
        growth = (long_time / long_bars) / (short_time / short_bars)
        record("scaling", f"{short_bars} sessions", short_bars, short_time, "")
        record("scaling", f"{long_bars} sessions", long_bars, long_time,
               f"cost per bar x{growth:.2f} over 4x the run")
        self.assertLess(
            growth, 2.0,
            f"per-bar cost grew {growth:.2f}x when the run got 4x longer ({short_bars} -> "
            f"{long_bars} sessions). Linear work per bar would leave this near 1.0; this is the "
            f"signature of something scanning the whole run on every bar.")


class FuturesSpeedTests(unittest.IsolatedAsyncioTestCase):
    """A futures contract, where each bar also carries a multiplier and daily settlement.

    Its own class because a contract has to be created in the database first -- the seeded one has
    no futures -- which is setup the equity harness does not need.
    """

    MULTIPLIER = 1000.0

    async def asyncSetUp(self):
        from tests.futures_fixtures import EXCHANGE
        from ziplime.assets.domain.settlement_type import SettlementType
        from ziplime.assets.entities.commodity import Commodity
        from ziplime.assets.entities.currency import Currency
        from ziplime.assets.entities.exchange_asset import ExchangeAsset
        from ziplime.assets.entities.futures_contract import FuturesContract

        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-speed-fut-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)

        self.start, self.end = datetime.date(2023, 2, 1), datetime.date(2023, 6, 30)
        bundle_start = datetime.date(2023, 1, 3)
        sessions = self.calendar.sessions_in_range(bundle_start, self.end)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        steps = np.arange(len(self.index))
        self.prices = pd.Series(75.0 + np.sin(steps / 9.0) * 6.0 + np.sin(steps / 2.5) * 1.5,
                                index=self.index)

        far_past, far_future = datetime.date(1900, 1, 1), datetime.date(2099, 1, 1)
        await self.asset_service.save_exchanges(exchanges=[EXCHANGE])
        # id=None so the database assigns one: the module fixtures carry ids the seeded database
        # already uses.
        [quote] = await self.asset_service.save_currencies([Currency(
            id=None, isin=None, asset_name="USD", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        [root] = await self.asset_service.save_commodities([Commodity(
            id=None, isin=None, asset_name="CL", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        expiration = datetime.date(2023, 12, 20)
        [contract] = await self.asset_service.save_futures_contracts([FuturesContract(
            id=None, isin=None, asset_name="CLZ23", start_date=bundle_start, end_date=expiration,
            first_traded=bundle_start, auto_close_date=expiration + datetime.timedelta(days=1),
            root_exchange_asset=None, root_asset=root, root_symbol="CL", notice_date=expiration,
            expiration_date=expiration, multiplier=self.MULTIPLIER, tick_size=0.01,
            settlement_type=SettlementType.CASH, margin_currency="USD")])
        [self.listing] = await self.asset_service.save_exchange_assets(
            exchange_assets=[ExchangeAsset(
                sid=None, symbol="CLZ23", start_date=bundle_start, end_date=expiration,
                first_traded=bundle_start,
                auto_close_date=expiration + datetime.timedelta(days=1),
                external_id="clz23", exchange=EXCHANGE, asset=contract, quote=quote)])

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def run_strategy(self, fixture: str):
        from ziplime.core.run_simulation import run_simulation

        frame = pd.DataFrame({"CLZ23": self.prices})
        bundle = build_bundle("speed-futures", frame, {"CLZ23": self.listing.sid},
                              self.calendar, datetime.timedelta(days=1), mic="XCME")
        with contextlib.redirect_stdout(io.StringIO()):
            return await run_simulation(
                start_date=datetime.datetime.combine(self.start, datetime.time.min,
                                                     tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(self.end, datetime.time.max,
                                                   tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                total_cash=1_000_000.0, market_data_source=bundle, custom_data_sources=[],
                algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None, equity_commission=NoCommission(),
                equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False)

    async def test_futures_run_both_ways_and_agree(self):
        timings = {}
        results = {}
        for label, fixture in (("vectorised", "signals_futures.py"),
                               ("bar-by-bar", "signals_futures_bar_by_bar.py")):
            best = None
            for _ in range(REPEATS):
                started = time.perf_counter()
                results[label] = await self.run_strategy(fixture)
                elapsed = time.perf_counter() - started
                best = elapsed if best is None else min(best, elapsed)
            timings[label] = best
            record("futures", label, len(results[label].perf), best, "")

        self.assertEqual(order_signatures(results["vectorised"]),
                         order_signatures(results["bar-by-bar"]),
                         "the two futures versions are not the same strategy")
        self.assertGreater(len(order_signatures(results["vectorised"])), 0,
                           "the futures strategy never traded")


class BondSpeedTests(unittest.IsolatedAsyncioTestCase):
    """A bond, where every bar also accrues interest and may pay a coupon.

    Bonds have no vectorised twin to compare against -- a coupon schedule is not a rolling window
    -- so this measures the event-driven cost alone. That is the point: it is the asset class
    where the per-bar work is largest and least avoidable.
    """

    async def asyncSetUp(self):
        from ziplime.data.data_sources.demo_bonds import build_demo_bond_universe, demo_exchange

        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-speed-bond-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        await self.asset_service.save_exchanges(exchanges=[demo_exchange()])
        await self.asset_service.import_assets(assets_import=build_demo_bond_universe())
        self.calendar = get_calendar(CALENDAR)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def test_a_bond_run_reports_its_throughput(self):
        import os

        from ziplime.core.run_simulation import run_simulation
        from ziplime.data.data_sources.demo_bonds import build_demo_bond_bars

        start, end = datetime.date(2023, 6, 1), datetime.date(2024, 6, 1)
        in_range = self.calendar.sessions_in_range(start, end)
        # `build_demo_bond_bars` stamps each bar with a plain `date`, and a bundle is expected to
        # carry datetimes -- `BarData.last_bar_session` calls `.date()` on what it finds. Restamp
        # each bar at its session's close, which is what the bond end-to-end tests do too.
        closes = {session.date(): close.to_pydatetime() for session, close in
                  self.calendar.schedule.loc[in_range, "close"]
                  .dt.tz_convert(self.calendar.tz).items()}
        listing = await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol="ZLB26", mic="XNYS"), asset_type=AssetType.BOND)
        bars = build_demo_bond_bars(listings=[listing], sessions=list(closes))
        bars = bars.with_columns(
            pl.col("date").map_elements(
                closes.__getitem__,
                return_dtype=pl.Datetime(time_unit="us", time_zone=str(self.calendar.tz)))
        ).sort(["sid", "date"])
        spans = bars.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        bundle = DataBundle(
            name="speed-bond", version="1", start_date=bars["date"].min(),
            end_date=bars["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=bars["date"].max(), data=bars,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1)
                         for r in spans.iter_rows(named=True)},
            asset_service=self.asset_service)

        os.environ["ZIPLIME_TEST_BOND_TICKER"] = "ZLB26"
        try:
            best, result = None, None
            for _ in range(REPEATS):
                started = time.perf_counter()
                with contextlib.redirect_stdout(io.StringIO()):
                    result = await run_simulation(
                        start_date=datetime.datetime.combine(start, datetime.time.min,
                                                             tzinfo=self.calendar.tz),
                        end_date=datetime.datetime.combine(end, datetime.time.max,
                                                           tzinfo=self.calendar.tz),
                        trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                        total_cash=1_000_000.0, market_data_source=bundle,
                        custom_data_sources=[],
                        algorithm_file=str(FIXTURES / "bond_buy_and_hold.py"),
                        stop_on_error=True, asset_service=self.asset_service,
                        benchmark_asset_symbol=None, benchmark_returns=None,
                        bond_commission=NoCommission(), bond_slippage=NoSlippage(),
                        equity_commission=NoCommission(), max_leverage=1.0,
                        same_bar_execution=True, price_used_in_order_execution="close",
                        print_algo=False)
                elapsed = time.perf_counter() - started
                best = elapsed if best is None else min(best, elapsed)
        finally:
            os.environ.pop("ZIPLIME_TEST_BOND_TICKER", None)

        record("bond", "buy-and-hold with a coupon schedule", len(result.perf), best, "")
        self.assertGreater(len(result.perf), 0)


class FundamentalsSpeedTests(unittest.IsolatedAsyncioTestCase):
    """A strategy whose signal is a filing rather than a price.

    The dataset is mounted the way a Hugging Face one is -- through
    :class:`~ziplime.data.data_sources.huggingface.huggingface_data_source.HuggingFaceDataSource`
    -- but built from a frame in the test rather than downloaded. Nothing here touches the
    network: a suite that needs the Hub stops being run, and what is being measured is the
    engine's per-bar cost, not the Hub's latency.

    The as-of join is why this class exists. A statement arrives four times a year while bars
    arrive every bar, so reading a fundamental at a bar means "the newest filing known by then,
    carried forward". Bar by bar that is a lookup per bar; vectorised it is one pass producing the
    whole column.

    Measured, that turns out to be worth almost nothing on a daily run -- about 1.05x, against
    2.5x for a price signal on minute bars. Worth stating plainly rather than quietly dropping,
    because the intuition points the other way: a fundamental *looks* like the expensive lookup.
    It is not. Seven filings resolve to a handful of distinct values, the per-bar lookup is a
    dictionary hit, and on daily bars there are only 167 of them -- so the saving lands on a part
    of the bar that was never the cost. The mechanism pays for bar *count*, and a fundamental
    strategy on daily bars has the fewest of any case here. The agreement assertion below is what
    this class is really worth keeping for.
    """

    TICKERS = ("JNJ", "KO")
    FIELDS = ["revenue", "gross_profit", "total_assets"]
    START, END = datetime.date(2024, 2, 1), datetime.date(2024, 9, 30)
    BUNDLE_START = datetime.date(2023, 11, 1)

    #: Shaped like the real thing: a period filed once, then restated by a later filing that
    #: carries fewer line items. The August filing is what makes JNJ the better business on this
    #: measure, so the strategy actually switches rather than buying once and holding.
    STATEMENTS = [
        ("JNJ", datetime.date(2023, 12, 15), 1000.0, 400.0, 5000.0),
        ("JNJ", datetime.date(2024, 3, 20), 1100.0, 450.0, 5200.0),
        ("JNJ", datetime.date(2024, 6, 18), 1150.0, 470.0, None),
        ("JNJ", datetime.date(2024, 8, 14), 1200.0, 700.0, 5600.0),
        ("KO", datetime.date(2023, 12, 20), 800.0, 320.0, 3000.0),
        ("KO", datetime.date(2024, 4, 11), 850.0, 360.0, 3100.0),
        ("KO", datetime.date(2024, 7, 9), 900.0, 360.0, None),
    ]

    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-speed-fund-")
        db = Path(self._temp.name) / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db)
        self.asset_service = get_asset_service(db_path=str(db))
        self.calendar = get_calendar(CALENDAR)
        sessions = self.calendar.sessions_in_range(self.BUNDLE_START, self.END)
        self.index = pd.DatetimeIndex(
            self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        self.listings = {}
        for ticker in self.TICKERS:
            self.listings[ticker] = await self.asset_service.get_exchange_asset_by_symbol(
                symbol=AssetSymbol(symbol=ticker, mic="XNYS"), asset_type=AssetType.EQUITY)
        self.sids = {t: listing.sid for t, listing in self.listings.items()}

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def source(self):
        from ziplime.data.data_sources.huggingface.huggingface_data_source import (
            HuggingFaceDataSource, Resolution,
        )

        frame = pl.DataFrame({
            "ticker": [row[0] for row in self.STATEMENTS],
            "knowledge_date": [
                datetime.datetime.combine(row[1], datetime.time(8, 0), tzinfo=self.calendar.tz)
                for row in self.STATEMENTS],
            "revenue": [row[2] for row in self.STATEMENTS],
            "gross_profit": [row[3] for row in self.STATEMENTS],
            "total_assets": [row[4] for row in self.STATEMENTS],
        })
        # `from_frame` is a plain constructor, not a coroutine; the enclosing method is async
        # only because the engine expects to await for a source.
        return HuggingFaceDataSource.from_frame(
            frame=frame, name="fundamentals", knowledge_column="knowledge_date",
            entity_column="ticker", event_column=None, asset_service=self.asset_service,
            start_date=self.BUNDLE_START, end_date=self.END,
            session_timezone=str(self.calendar.tz), fields=self.FIELDS,
            resolution=Resolution.COALESCE)

    async def run_strategy(self, fixture: str):
        from ziplime.core.run_simulation import run_simulation

        steps = np.arange(len(self.index))
        frame = pd.DataFrame({"JNJ": 150.0 + np.sin(steps / 13.0) * 5.0,
                              "KO": 60.0 + np.cos(steps / 9.0) * 3.0}, index=self.index)
        bundle = build_bundle("speed-fund", frame, self.sids, self.calendar,
                              datetime.timedelta(days=1))
        with contextlib.redirect_stdout(io.StringIO()):
            return await run_simulation(
                start_date=datetime.datetime.combine(self.START, datetime.time.min,
                                                     tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(self.END, datetime.time.max,
                                                   tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                total_cash=CASH, market_data_source=bundle,
                custom_data_sources=[await self.source()],
                algorithm_file=str(FIXTURES / fixture), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None, equity_commission=NoCommission(),
                equity_slippage=NoSlippage(), max_leverage=10.0, print_algo=False)

    async def time_strategy(self, fixture: str):
        best, result = None, None
        for _ in range(REPEATS):
            started = time.perf_counter()
            result = await self.run_strategy(fixture)
            elapsed = time.perf_counter() - started
            best = elapsed if best is None else min(best, elapsed)
        return best, result

    async def test_both_paths_rank_on_the_same_fundamentals(self):
        """The agreement that has to hold before the timing below means anything, and the one
        most easily broken: an as-of join that is inclusive rather than strict hands the
        vectorised path a filing hours before the bar-by-bar path can see it."""
        fast_time, fast = await self.time_strategy("fundamentals_vectorised.py")
        slow_time, slow = await self.time_strategy("fundamentals_bar_by_bar.py")

        placed = order_signatures(fast)
        self.assertGreater(len(placed), 2, "the rotation never traded, so agreement proves nothing")
        self.assertEqual(placed, order_signatures(slow),
                         "the two fundamental paths are reading different statements")

        speedup = slow_time / fast_time
        record("fundamentals", "vectorised as-of panel", len(fast.perf), fast_time,
               f"{speedup:.2f}x faster than per-bar lookup")
        record("fundamentals", "per-bar lookup", len(slow.perf), slow_time, "")
        self.assertLess(fast_time, slow_time * MAXIMUM_PENALTY,
                        "the vectorised as-of panel is now costing more than the per-bar lookup")
