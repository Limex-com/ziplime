"""A sweep must reproduce the engine, combination by combination.

Each test sweeps a small grid of one strategy in float64 and then runs every combination through
run_simulation, requiring the same final portfolio value (to the cent) and the same number of
trades. The strategies in tests/fixtures/sweep each exercise one part of what the sweep simulates:
indicators, history windows, shorts and every order helper, scheduled functions and
before_trading_start, limit and stop orders with both cancel policies, splits and dividends,
futures. The bars are synthetic and deterministic, with the awkward cases switched on where they
matter: missing bars (the engine then reads the previous row), midnight timestamps (history then
includes the current bar), thin volume (fills capped by FixedBasisPointsSlippage).
"""
import contextlib
import datetime
import io
import json
import shutil
import sqlite3
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
from ziplime.core.run_simulation import run_simulation
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.sweep import SweepError, SweepParams, run_sweep
from ziplime.utils.calendar_utils import get_calendar

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "sweep"
CALENDAR = "XNYS"
START, END = datetime.date(2024, 1, 2), datetime.date(2024, 5, 31)
HISTORY_FROM = datetime.date(2023, 10, 2)


#: The GPU backend the GPU tests run on: mlx (Apple, or NVIDIA with mlx[cuda]), cupy or dpnp
GPU = __import__("os").environ.get("ZIPLIME_SWEEP_TEST_GPU", "mlx")


#: The grid of signals_cross.py, the one fixture whose decisions are entries/exits signals.
SIGNALS_GRID = {"fast": [3, 5, 8], "slow": [15, 25]}


def mlx_available() -> bool:
    """Whether the GPU backend under test is installed (the name is kept for older imports)."""
    try:
        __import__({"mlx": "mlx.core"}.get(GPU, GPU))
        return True
    except ImportError:
        return False


def compiles() -> bool:
    """Whether the GPU tests' sweeps compile their bars: MLX on an Apple GPU."""
    from ziplime.sweep.params import SweepParams
    from ziplime.sweep.runner import compile_step
    return bool(compile_step(SweepParams.model_validate(
        {"algorithm_file": "x", "start_date": "2024-01-02", "grid": {"x": [0]}, "backend": GPU})))


class SweepParityCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="ziplime-sweep-parity-")
        self.folder = Path(self._temp.name)
        self.db = self.folder / "assets.sqlite"
        shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", self.db)
        self.asset_service = get_asset_service(db_path=str(self.db))
        self.calendar = get_calendar(CALENDAR)

    async def asyncTearDown(self):
        await self.asset_service._asset_repository.engine.dispose()
        self._temp.cleanup()

    async def listing(self, symbol, asset_type=AssetType.EQUITY):
        ticker, mic = symbol.split("@")
        return await self.asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=asset_type)

    def bundle(self, listings, *, seed=7, gaps=(), midnight=False, thin=False, start=HISTORY_FROM,
               base_price=50.0):
        sessions = self.calendar.sessions_in_range(start, END)
        if midnight:
            stamps = [datetime.datetime.combine(s.date(), datetime.time.min, tzinfo=self.calendar.tz)
                      for s in sessions]
        else:
            stamps = list(self.calendar.schedule.loc[sessions, "close"].dt.tz_convert(self.calendar.tz))
        rng = np.random.default_rng(seed)
        rows = []
        for k, listing in enumerate(listings):
            prices = base_price * (1 + k * 0.3) * np.exp(np.cumsum(rng.normal(0, 0.015, len(stamps))))
            volume = (rng.integers(200, 900, len(stamps)) if thin
                      else rng.integers(200_000, 2_000_000, len(stamps))).astype(float)
            for t, stamp in enumerate(stamps):
                if listing.symbol in gaps and rng.random() < 0.08 and stamp.date() >= START:
                    continue  # no row: the engine reads the previous one
                p = round(float(prices[t]), 2)
                rows.append({"date": stamp, "sid": listing.sid, "symbol": listing.symbol,
                             "mic": listing.mic, "open": round(p * 0.997, 2), "high": p, "low": p,
                             "close": p, "price": p, "volume": float(volume[t])})
        data = pl.DataFrame(rows).sort(["sid", "date"])
        spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
            [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
        return DataBundle(
            name="sweep-parity", version="1", start_date=data["date"].min(),
            end_date=data["date"].max(), trading_calendar=self.calendar,
            frequency=datetime.timedelta(days=1), original_frequency=datetime.timedelta(days=1),
            data_type=DataType.MARKET_DATA, timestamp=data["date"].max(), data=data,
            sid_indexes={r["sid"]: (r["first"], r["last"] + 1) for r in spans.iter_rows(named=True)})

    async def sweep(self, fixture, grid, bundle, *, backend="exact", say=None,
                    **costs) -> pd.DataFrame:
        # A strategy the sweep cannot convert would otherwise run on the engine and agree with it
        # trivially; the one test of that way out asks for it.
        costs.setdefault("on_unsupported", "refuse")
        params = SweepParams.model_validate({
            "algorithm_file": str(FIXTURES / fixture), "start_date": str(START),
            "end_date": str(END), "trading_calendar": CALENDAR, "total_cash": 100_000,
            "grid": grid, "backend": backend, "verify": False, "refine_top": 0,
            "output_dir": str(self.folder / "results"), **costs})
        with contextlib.redirect_stdout(io.StringIO()):
            return await run_sweep(params, self.folder, say=say or (lambda message: None),
                                   market_data=bundle, asset_service=self.asset_service)

    async def assert_signals_kernel_agrees(self, backend):
        """signals_cross.py on a GPU backend against exact: the sweep takes the kernel's path,
        and the fills and values agree."""
        listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        bundle = self.bundle(listings, seed=23)
        said = []
        exact = (await self.sweep("signals_cross.py", SIGNALS_GRID, bundle)).sort_index()
        gpu = (await self.sweep("signals_cross.py", SIGNALS_GRID, bundle, backend=backend,
                                say=said.append)).sort_index()
        self.assertTrue(any("decisions: entries/exits signals" in line for line in said),
                        "the sweep did not take the signals kernel's path")
        self.assertGreater(int(exact["fills"].sum()), 0)
        np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
        np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)

    async def engine(self, fixture, config: dict, bundle, *, timing="next_close", **costs):
        path = self.folder / "config.json"
        path.write_text(json.dumps(config))
        params = SweepParams.model_validate({"algorithm_file": "x", "start_date": str(START),
                                             "grid": {"x": [0]}, **costs})
        with contextlib.redirect_stdout(io.StringIO()):
            result = await run_simulation(
                start_date=datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz),
                end_date=datetime.datetime.combine(END, datetime.time.max, tzinfo=self.calendar.tz),
                trading_calendar=CALENDAR, emission_rate=datetime.timedelta(days=1),
                total_cash=100_000.0, market_data_source=bundle, custom_data_sources=[],
                algorithm_file=str(FIXTURES / fixture), config_file=str(path), stop_on_error=True,
                asset_service=self.asset_service, benchmark_asset_symbol=None,
                benchmark_returns=None,
                equity_commission=params.equity_commission.model(),
                equity_slippage=params.equity_slippage.model(),
                future_commission=params.future_commission.model(),
                future_slippage=params.future_slippage.model(),
                same_bar_execution=timing == "same_close",
                price_used_in_order_execution="open" if timing == "next_open" else "close",
                max_leverage=100.0, print_algo=False)
        self.assertFalse(result.errors, [e.message for e in result.errors][:3])
        perf = result.perf
        return (float(perf["portfolio_value"].iloc[-1]),
                sum(len(row) for row in perf["transactions"]))

    async def assert_parity(self, fixture, grid, bundle, **costs):
        table = await self.sweep(fixture, grid, bundle, **costs)
        timing = costs.get("timing", "next_close")
        traded = 0
        for _, row in table.iterrows():
            config = {name: (row[name].item() if hasattr(row[name], "item") else row[name])
                      for name in grid}
            value, trades = await self.engine(fixture, config, bundle, **costs)
            traded += trades
            with self.subTest(config=config):
                self.assertAlmostEqual(row["final_value"], value, delta=0.01)
                self.assertEqual(int(row["fills"]), trades)
        self.assertGreater(traded, 0, "the grid traded nothing -- the test would prove nothing")
        return table


class EquityParityTests(SweepParityCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]

    async def test_indicators_and_target_percent(self):
        await self.assert_parity("trend.py", {"fast": [3, 5], "slow": [12, 20]},
                                 self.bundle(self.listings),
                                 equity_commission={"type": "per_share", "cost": 0.005,
                                                    "min_trade_cost": 1.0})

    async def test_missing_bars_read_the_previous_row(self):
        await self.assert_parity("trend.py", {"fast": [3], "slow": [10, 15], "rebalance_every": [1]},
                                 self.bundle(self.listings, gaps=("KO",), seed=11))

    async def test_thin_volume_caps_fills(self):
        await self.assert_parity("trend.py", {"fast": [3], "slow": [10], "rebalance_every": [1, 4]},
                                 self.bundle(self.listings, thin=True, seed=3, base_price=20.0),
                                 equity_slippage={"type": "fixed_bps", "basis_points": 5,
                                                  "volume_limit": 0.1},
                                 equity_commission={"type": "per_share", "cost": 0.01,
                                                    "min_trade_cost": 1.0})

    async def test_same_bar_and_next_open_execution(self):
        bundle = self.bundle(self.listings, seed=5)
        for timing in ("same_close", "next_open"):
            with self.subTest(timing=timing):
                await self.assert_parity("trend.py", {"fast": [3], "slow": [10, 14]}, bundle,
                                         timing=timing)

    async def test_history_windows(self):
        await self.assert_parity("history.py", {"window": [5, 15], "band": [0.5, 1.0]},
                                 self.bundle(self.listings, seed=9),
                                 equity_commission={"type": "per_dollar", "cost": 0.001})

    async def test_history_includes_the_current_bar_when_bars_are_stamped_at_midnight(self):
        await self.assert_parity("history.py", {"window": [5, 10], "band": [0.5]},
                                 self.bundle(self.listings, seed=9, midnight=True))

    async def test_shorts_and_every_order_helper(self):
        await self.assert_parity("shorts.py", {"lookback": [5, 10], "size": [0.1, 0.3]},
                                 self.bundle(self.listings, seed=21),
                                 equity_commission={"type": "per_trade", "cost": 2.0})

    async def test_selection_by_sorted_returns(self):
        listings = self.listings + [await self.listing("XOM@XNYS")]
        await self.assert_parity("selection.py", {"lookback": [10, 30], "top_k": [1, 2]},
                                 self.bundle(listings, seed=17))

    async def test_history_over_several_instruments(self):
        listings = self.listings + [await self.listing("XOM@XNYS")]
        await self.assert_parity("history_frame.py", {"lookback": [5, 20], "top_k": [1, 2]},
                                 self.bundle(listings, seed=23, gaps={"KO"}))

    @unittest.skipUnless(__import__("importlib").util.find_spec("talib"), "needs TA-Lib")
    async def test_decision_tree_on_history_arrays_and_named_instruments(self):
        listings = self.listings + [await self.listing("XOM@XNYS")]
        await self.assert_parity("tree.py", {"drawdown_limit": [4.0, 8.0], "vol_limit": [1.0, 1.5],
                                             "rsi_period": [7, 14], "momentum_window": [5, 20]},
                                 self.bundle(listings, seed=29, gaps={"KO"}))

    async def test_history_computation_whose_parameter_changes_during_the_run(self):
        await self.assert_parity("history_varying.py", {"window": [5, 10]},
                                 self.bundle(self.listings, seed=37))

    def replay_stats(self):
        """How the sweeps in a test computed their records on history (LazyRegistry.stats)."""
        from ziplime.sweep import lazy
        seen = {}
        original = lazy.LazyRegistry.prepare

        def prepare(registry):
            original(registry)
            for name, count in registry.stats.items():
                seen[name] = max(seen.get(name, 0), count)
        lazy.LazyRegistry.prepare = prepare
        self.addCleanup(setattr, lazy.LazyRegistry, "prepare", original)
        return seen

    @unittest.skipUnless(__import__("importlib").util.find_spec("talib"), "needs TA-Lib")
    async def test_ta_lib_on_the_whole_history(self):
        stats = self.replay_stats()
        await self.assert_parity("roc_pair.py", {"lookback": [5, 20, 40], "gap": [0.0, 1.0]},
                                 self.bundle(self.listings[:2], seed=5))
        self.assertGreater(stats["whole history"], 0)
        self.assertEqual(stats["replayed"] + stats["replayed on cores"], 0)

    @unittest.skipUnless(__import__("importlib").util.find_spec("talib"), "needs TA-Lib")
    async def test_replayed_on_the_cpus_cores(self):
        from ziplime.sweep import lazy
        stats = self.replay_stats()
        self.addCleanup(setattr, lazy, "PARALLEL_FROM", lazy.PARALLEL_FROM)
        lazy.PARALLEL_FROM = 0
        listings = self.listings + [await self.listing("XOM@XNYS")]
        await self.assert_parity("tree.py", {"rsi_period": [7, 14], "momentum_window": [5, 20]},
                                 self.bundle(listings, seed=29, gaps={"KO"}))
        self.assertGreater(stats["replayed on cores"], 0)

    @unittest.skipUnless(__import__("importlib").util.find_spec("sklearn"), "needs scikit-learn")
    async def test_a_model_fitted_in_initialize(self):
        await self.assert_parity("model.py", {"strength": [0.01, 1.0, 100.0],
                                              "threshold": [0.45, 0.5, 0.55]},
                                 self.bundle(self.listings[:2], seed=41))

    async def test_a_frame_of_several_instruments_read_as_one_is_refused(self):
        from ziplime.sweep.params import SweepError

        with self.assertRaisesRegex(SweepError, "mixes their rows"):
            await self.sweep("history_pooled.py", {"window": [5]}, self.bundle(self.listings))

    async def test_leader_by_max_and_rebalance(self):
        await self.assert_parity("leader.py", {"lookback": [10, 25], "weight": [0.5, 0.9]},
                                 self.bundle(self.listings, seed=19))

    async def test_per_instrument_state_on_the_context(self):
        await self.assert_parity("stops.py", {"stop": [0.02, 0.05], "hold": [5, 12]},
                                 self.bundle(self.listings, seed=13))

    async def test_limit_and_stop_orders_with_both_cancel_policies(self):
        await self.assert_parity("limits.py", {"discount": [0.0, 0.01], "eod": [False, True]},
                                 self.bundle(self.listings[:2], seed=4))


class EngineFallbackTests(SweepParityCase):
    """A strategy the sweep cannot convert still gets swept: each combination through the engine."""

    async def test_unconvertible_strategy_runs_on_the_engine(self):
        listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        await self.assert_parity("trend_pandas.py", {"fast": [3, 5], "slow": [12, 20]},
                                 self.bundle(listings, seed=7), workers=2,
                                 on_unsupported="engine")

    async def test_refuse_stops_instead(self):
        from ziplime.sweep.params import SweepError

        listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        with self.assertRaisesRegex(SweepError, "pandas"):
            await self.sweep("trend_pandas.py", {"fast": [3]}, self.bundle(listings, seed=7),
                             on_unsupported="refuse")


class ScheduleParityTests(SweepParityCase):
    async def test_scheduled_function_and_before_trading_start(self):
        listing = await self.listing("JNJ@XNYS")
        await self.assert_parity("scheduled.py", {"offset": [0, 2], "weight": [0.3, 0.6]},
                                 self.bundle([listing], seed=8))


class CorporateActionParityTests(SweepParityCase):
    async def test_split_with_cash_in_lieu_and_a_dividend(self):
        jnj, ko = await self.listing("JNJ@XNYS"), await self.listing("KO@XNYS")
        with sqlite3.connect(self.db) as connection:
            connection.execute("INSERT INTO splits (asset_id, effective_date, ratio) VALUES (?, ?, ?)",
                               (jnj.asset.id, "2024-03-04", 1.5))
            connection.execute(
                "INSERT INTO dividend_payouts (asset_id, ex_date, declared_date, record_date, "
                "pay_date, amount) VALUES (?, ?, ?, ?, ?, ?)",
                (ko.asset.id, "2024-02-14", "2024-02-01", "2024-02-15", "2024-04-01", 0.97))
        await self.asset_service._asset_repository.engine.dispose()
        self.asset_service = get_asset_service(db_path=str(self.db))
        jnj, ko = await self.listing("JNJ@XNYS"), await self.listing("KO@XNYS")
        await self.assert_parity("corporate.py", {"shares": [100, 155], "weight": [0.4]},
                                 self.bundle([jnj, ko], seed=6))


class FuturesParityTests(SweepParityCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        from tests.futures_fixtures import EXCHANGE
        from ziplime.assets.domain.settlement_type import SettlementType
        from ziplime.assets.entities.commodity import Commodity
        from ziplime.assets.entities.currency import Currency
        from ziplime.assets.entities.exchange_asset import ExchangeAsset
        from ziplime.assets.entities.futures_contract import FuturesContract

        far_past, far_future = datetime.date(1900, 1, 1), datetime.date(2099, 1, 1)
        await self.asset_service.save_exchanges(exchanges=[EXCHANGE])
        [quote] = await self.asset_service.save_currencies([Currency(
            id=None, isin=None, asset_name="USD", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        [root] = await self.asset_service.save_commodities([Commodity(
            id=None, isin=None, asset_name="CL", start_date=far_past, end_date=far_future,
            first_traded=far_past, auto_close_date=far_future)])
        expiration = datetime.date(2024, 12, 20)
        [contract] = await self.asset_service.save_futures_contracts([FuturesContract(
            id=None, isin=None, asset_name="CLZ23", start_date=HISTORY_FROM, end_date=expiration,
            first_traded=HISTORY_FROM, auto_close_date=expiration, root_exchange_asset=None,
            root_asset=root, root_symbol="CL", notice_date=expiration, expiration_date=expiration,
            multiplier=1000.0, tick_size=0.01, settlement_type=SettlementType.CASH,
            margin_currency="USD")])
        [self.contract] = await self.asset_service.save_exchange_assets(exchange_assets=[ExchangeAsset(
            sid=None, symbol="CLZ23", start_date=HISTORY_FROM, end_date=expiration,
            first_traded=HISTORY_FROM, auto_close_date=expiration, external_id="clz23",
            exchange=EXCHANGE, asset=contract, quote=quote)])

    async def test_futures_settle_daily_with_the_multiplier(self):
        await self.assert_parity("futures.py", {"fast": [3, 5], "contracts": [1, 3]},
                                 self.bundle([self.contract], seed=12, base_price=75.0),
                                 future_commission={"type": "per_contract", "cost": 0.85,
                                                    "exchange_fee": 1.5})


@unittest.skipUnless(mlx_available(), "MLX is not installed")
class GpuAgreesWithExactTests(SweepParityCase):
    """float32 on the GPU against float64 on the CPU: the same fills, values within a cent per
    ten thousand dollars -- the path can only diverge through rounding, which refine_top repairs."""

    async def test_signals_strategy_runs_the_kernel_on_the_gpu(self):
        """entries/exits send a sweep to simulate_signals_batch, not through handle_data: a GPU
        kernel of its own (Metal for mlx, CUDA for cupy), which the other fixtures never reach."""
        from ziplime.vectorized.kernel import _mlx
        if GPU == "mlx" and not _mlx._has_metal():
            # MLX on CUDA has no Metal to run this kernel on; the refusal names the one that does
            listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
            with self.assertRaisesRegex(SweepError, "backend='cupy'"):
                await self.sweep("signals_cross.py", SIGNALS_GRID, self.bundle(listings, seed=23),
                                 backend=GPU)
            return
        await self.assert_signals_kernel_agrees(GPU)

    async def test_gpu_matches_exact(self):
        listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        bundle = self.bundle(listings, seed=9)
        wide = self.bundle(listings + [await self.listing("XOM@XNYS")], seed=17)
        exact = (await self.sweep("selection.py", {"lookback": [10, 30], "top_k": [1, 2]}, wide)).sort_index()
        gpu = (await self.sweep("selection.py", {"lookback": [10, 30], "top_k": [1, 2]}, wide,
                                backend=GPU)).sort_index()
        np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
        np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)
        grid = {"lookback": [5, 20], "top_k": [1, 2]}
        exact = (await self.sweep("history_frame.py", grid, wide)).sort_index()
        gpu = (await self.sweep("history_frame.py", grid, wide, backend=GPU)).sort_index()
        np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
        np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)
        if __import__("importlib").util.find_spec("talib"):
            grid = {"drawdown_limit": [4.0, 8.0], "rsi_period": [7, 14], "momentum_window": [5, 20]}
            exact = (await self.sweep("tree.py", grid, wide)).sort_index()
            gpu = (await self.sweep("tree.py", grid, wide, backend=GPU)).sort_index()
            np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
            np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)
        # A parameter of a computation on history that changes during the run: the compiled run
        # notices and the batch runs plain
        from ziplime.sweep import engine
        plain_again = []
        original = engine._Simulation.go

        def go(simulation, compiled):
            try:
                return original(simulation, compiled)
            except engine.RunPlain:
                plain_again.append(True)
                raise
        engine._Simulation.go = go
        try:
            grid = {"window": [5, 10]}
            exact = (await self.sweep("history_varying.py", grid, bundle)).sort_index()
            gpu = (await self.sweep("history_varying.py", grid, bundle, backend=GPU)).sort_index()
        finally:
            engine._Simulation.go = original
        if compiles():
            self.assertTrue(plain_again, "the compiled run did not notice the changed parameter")
        np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
        np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)
        find = __import__("importlib").util.find_spec
        for fixture, grid, needs in (("roc_pair.py", {"lookback": [5, 20, 40], "gap": [0.0, 1.0]},
                                      "talib"),
                                     ("model.py", {"strength": [0.01, 1.0], "threshold": [0.45, 0.5]},
                                      "sklearn")):
            if not find(needs):
                continue
            pair = self.bundle(listings[:2], seed=41)
            exact = (await self.sweep(fixture, grid, pair)).sort_index()
            gpu = (await self.sweep(fixture, grid, pair, backend=GPU)).sort_index()
            with self.subTest(fixture=fixture):
                np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
                np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)
        for fixture, grid in (("trend.py", {"fast": [3, 5], "slow": [12, 20]}),
                              ("stops.py", {"stop": [0.02, 0.05], "hold": [5, 12]}),
                              ("leader.py", {"lookback": [10, 25], "weight": [0.5, 0.9]}),
                              ("history.py", {"window": [5, 15], "band": [0.5, 1.0]})):
            exact = (await self.sweep(fixture, grid, bundle)).sort_index()
            gpu = (await self.sweep(fixture, grid, bundle, backend=GPU)).sort_index()
            with self.subTest(fixture=fixture):
                np.testing.assert_array_equal(gpu["fills"].to_numpy(), exact["fills"].to_numpy())
                np.testing.assert_allclose(gpu["final_value"], exact["final_value"], rtol=1e-6)


class OpenClSweepTests(SweepParityCase):
    """backend opencl: the entries/exits kernel on any OpenCL GPU, and nothing else."""

    @unittest.skipUnless(__import__("importlib").util.find_spec("pyopencl")
                         and __import__("ziplime.vectorized.kernel._opencl",
                                        fromlist=["x"])._gpu_devices(),
                         "no OpenCL GPU: needs PyOpenCL and a GPU's OpenCL driver")
    async def test_signals_strategy_runs_the_kernel_on_opencl(self):
        await self.assert_signals_kernel_agrees("opencl")

    async def test_a_handle_data_strategy_is_refused(self):
        """The handle_data path needs an array library on the GPU, which PyOpenCL is not."""
        listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        with self.assertRaisesRegex(SweepError, "entries/exits signals only"):
            await self.sweep("trend.py", {"fast": [3, 5], "slow": [12, 20]},
                             self.bundle(listings), backend="opencl")


class BackendRefusalTests(unittest.TestCase):
    def test_dpnp_without_dpnp_says_where_it_comes_from(self):
        """dpnp is not on PyPI's index for most platforms; the refusal names Intel's."""
        from unittest import mock

        from ziplime.sweep.backends import Backend
        with mock.patch.dict("sys.modules", {"dpnp": None}):
            with self.assertRaisesRegex(SweepError, "software.repos.intel.com"):
                Backend("dpnp")


if __name__ == "__main__":
    unittest.main()
