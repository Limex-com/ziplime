"""Synthetic price paths: each method draws what it says it draws, a path is a bundle the engine
runs as it runs the real one, and a sweep over the paths reports what each combination did on
every path.

The GPU tests run on ZIPLIME_SWEEP_TEST_GPU (mlx by default) and are skipped without it.
"""
import datetime
import json
import sqlite3
import unittest

import numpy as np
import polars as pl

from tests.sweep.test_sweep_parity import (
    END, FIXTURES, GPU, START, SweepParityCase, mlx_available,
)
from ziplime.core.ingest_data import get_asset_service
from ziplime.sweep import synthetic
from ziplime.sweep.params import Family, SweepError, Synthetic


class Draws:
    """A generator whose uniforms are given."""

    def __init__(self, values):
        self.values = np.asarray(values, dtype=float)

    def random(self, shape):
        return self.values.reshape(shape)


def bars_of(close: np.ndarray, start: int, *, seed: int = 0) -> synthetic.Bars:
    """Bars side by side from closes, with an open, high, low and volume around each."""
    rng = np.random.default_rng(seed)
    rows, columns = close.shape
    opened = close * np.exp(rng.normal(0, 0.003, close.shape))
    fields = {"close": close, "open": opened,
              "high": np.maximum(close, opened) * (1 + rng.uniform(0, 0.01, close.shape)),
              "low": np.minimum(close, opened) * (1 - rng.uniform(0, 0.01, close.shape)),
              "volume": rng.integers(1_000, 9_000, close.shape).astype(float)}
    bar, column = np.divmod(np.arange(rows * columns), columns)
    return synthetic.Bars(fields=fields, rows=np.arange(rows * columns), bar=bar, column=column,
                          start=start)


def random_walk(rows=400, columns=3, seed=1, drift=0.0005, correlation=0.6):
    rng = np.random.default_rng(seed)
    covariance = 0.0002 * (correlation + (1 - correlation) * np.eye(columns))
    steps = rng.multivariate_normal(np.full(columns, drift), covariance, rows)
    return 50 * np.exp(np.cumsum(steps, axis=0))


class MebootTests(unittest.TestCase):
    def test_vinods_worked_example(self):
        # Vinod & López-de-Lacalle (2009), Table 1: x = 4, 12, 36, 20, 8 and the draws
        # 0.12, 0.83, 0.53, 0.59, 0.11 give 5.85, 13.90, 23.95, 15.70, 6.70 -- with the tails'
        # means kept (reachbnd = FALSE) and no expansion
        x = np.array([4.0, 12, 36, 20, 8])
        found = synthetic.meboot(x, Draws([0.12, 0.83, 0.53, 0.59, 0.11]), reachbnd=False,
                                 expand_sd=False)[0]
        np.testing.assert_allclose(found, [5.85, 13.90, 23.95, 15.70, 6.70])

    def test_ranks_bounds_and_mean(self):
        x = np.log(random_walk(250, 1, seed=3)[:, 0])
        replicates = synthetic.meboot(x, np.random.default_rng(0), reps=400, expand_sd=False)
        ranks = np.argsort(np.argsort(x))
        for replicate in replicates[:20]:
            np.testing.assert_array_equal(np.argsort(np.argsort(replicate)), ranks)
        changes = np.sort(np.abs(np.diff(x)))
        cut = int(len(changes) * 0.10)
        trimmed = changes[cut:len(changes) - cut].mean()
        self.assertGreaterEqual(replicates.min(), x.min() - trimmed - 1e-12)
        self.assertLessEqual(replicates.max(), x.max() + trimmed + 1e-12)
        # Mean-preserving: the ensemble's mean is the data's
        self.assertAlmostEqual(replicates.mean(), x.mean(), delta=x.std() * 0.01)

    def test_expand_sd_keeps_the_spread_at_least_the_datas(self):
        x = np.log(random_walk(120, 1, seed=4)[:, 0])
        replicates = synthetic.meboot(x, np.random.default_rng(1), reps=200)
        spread = replicates.std(axis=1, ddof=1)
        self.assertTrue((spread >= x.std(ddof=1) * (1 - 1e-9)).all())


class MethodTests(unittest.TestCase):
    def setUp(self):
        self.bars = bars_of(random_walk(), start=100)
        self.feats = synthetic.features(self.bars)
        self.pool = np.arange(1, self.bars.shape[0])

    def path(self, family: Family, seed=0, pool=None):
        made = synthetic.generator(family, self.feats, self.pool if pool is None else pool,
                                   self.bars.start)
        return made.path(np.random.default_rng(seed))

    def test_every_method_makes_consistent_bars_after_a_real_warmup(self):
        for method in ("iid", "shuffle", "block", "circular", "stationary", "meboot", "gbm",
                       "student_t", "garch"):
            with self.subTest(method=method):
                source, returns = self.path(Family(method=method))
                self.assertEqual(returns.shape, (300, 3))
                self.assertTrue(np.isfinite(returns).all())
                bars = synthetic.build(self.bars, self.feats, source, returns)
                for field, values in bars.items():
                    np.testing.assert_array_equal(values[:100], self.bars.fields[field][:100])
                np.testing.assert_allclose(np.diff(np.log(bars["close"][99:]), axis=0), returns,
                                           atol=1e-12)
                self.assertTrue((bars["high"] >= np.maximum(bars["open"], bars["close"])).all())
                self.assertTrue((bars["low"] <= np.minimum(bars["open"], bars["close"])).all())
                self.assertTrue((bars["volume"][100:] > 0).all())
                again = self.path(Family(method=method))
                np.testing.assert_array_equal(again[1], returns)

    def test_resampled_bars_are_real_bars_of_the_same_date_for_every_instrument(self):
        for method in ("iid", "shuffle", "block", "circular", "stationary"):
            with self.subTest(method=method):
                source, returns = self.path(Family(method=method, block=7))
                np.testing.assert_array_equal(returns, self.feats.returns[source])
                bars = synthetic.build(self.bars, self.feats, source, returns)
                # The bar keeps its shape: high and low against the close as on its real date
                np.testing.assert_allclose(bars["high"][100:] / bars["close"][100:],
                                           np.maximum(self.bars.fields["high"][source]
                                                      / self.bars.fields["close"][source],
                                                      bars["open"][100:] / bars["close"][100:]))

    def test_blocks(self):
        source, _ = self.path(Family(method="block", block=10))
        steps = np.diff(source).reshape(-1)
        # Inside a block of 10 every bar follows the one before it
        self.assertTrue((np.diff(source[:300].reshape(30, 10), axis=1) == 1).all())
        self.assertLess((steps == 1).mean(), 1.0)
        follows = np.mean([(np.diff(self.path(Family(method="stationary", block=10), seed=k)[0])
                            == 1).mean() for k in range(40)])
        # A new block starts with probability 1/10 (and wrapping or an unlucky start look alike)
        self.assertAlmostEqual(follows, 0.9, delta=0.03)

    def test_shuffle_of_the_run_ends_where_the_real_run_ends(self):
        run = np.arange(self.bars.start, self.bars.shape[0])
        source, returns = self.path(Family(method="shuffle"), pool=run)
        self.assertEqual(sorted(source.tolist()), run.tolist())
        bars = synthetic.build(self.bars, self.feats, source, returns)
        np.testing.assert_allclose(bars["close"][-1], self.bars.fields["close"][-1])

    def test_zero_drift(self):
        for method in ("stationary", "gbm", "garch", "meboot"):
            with self.subTest(method=method):
                means = np.mean([self.path(Family(method=method, drift="zero"), seed=k)[1].mean(axis=0)
                                 for k in range(60)], axis=0)
                np.testing.assert_allclose(means, 0, atol=2.5e-4)

    def test_gaussian_paths_have_the_real_covariance(self):
        long = bars_of(random_walk(3000, 3, seed=5), start=1500)
        feats = synthetic.features(long)
        pool = np.arange(1, 3000)
        made = synthetic.generator(Family(method="gbm"), feats, pool, long.start)
        drawn = np.vstack([made.path(np.random.default_rng(k))[1] for k in range(20)])
        np.testing.assert_allclose(np.cov(drawn, rowvar=False), np.cov(feats.returns[pool],
                                                                       rowvar=False), rtol=0.06)
        t = synthetic.generator(Family(method="student_t", df=4), feats, pool, long.start)
        drawn_t = np.vstack([t.path(np.random.default_rng(k))[1] for k in range(20)])
        # Fat tails at the same variance
        kurtosis = ((drawn_t - drawn_t.mean(0)) ** 4).mean(0) / drawn_t.var(0) ** 2
        self.assertTrue((kurtosis > 4).all())
        np.testing.assert_allclose(drawn_t.var(0), feats.returns[pool].var(0), rtol=0.15)

    def test_garch_fit_recovers_its_parameters(self):
        rng = np.random.default_rng(11)
        omega, alpha, beta = 2e-6, 0.10, 0.85
        residuals, variance = np.empty(6000), omega / (1 - alpha - beta)
        for t in range(6000):
            residuals[t] = np.sqrt(variance) * rng.standard_normal()
            variance = omega + alpha * residuals[t] ** 2 + beta * variance
        _, a, b = synthetic.fit_garch(residuals)
        self.assertAlmostEqual(a, alpha, delta=0.03)
        self.assertAlmostEqual(b, beta, delta=0.04)

    def test_a_late_listing_starts_at_its_own_first_close(self):
        close = random_walk()
        close[:150, 2] = np.nan
        bars = bars_of(close, start=100)
        feats = synthetic.features(bars)
        for method in ("stationary", "meboot", "garch"):
            with self.subTest(method=method):
                made = synthetic.generator(Family(method=method), feats, np.arange(1, 400), 100)
                source, returns = made.path(np.random.default_rng(0))
                path = synthetic.build(bars, feats, source, returns)
                self.assertAlmostEqual(path["close"][150, 2], close[150, 2])
                self.assertTrue(np.isfinite(path["close"][150:, 2]).all())


class SettingsTests(unittest.TestCase):
    def test_labels_and_choices(self):
        self.assertEqual(Family(method="stationary", block=15).label, "stationary(15)")
        self.assertEqual(Family(method="meboot", drift="zero").label, "meboot, zero drift")
        with self.assertRaises(ValueError):
            Synthetic(families=[Family(method="gbm"), Family(method="gbm")])
        with self.assertRaises(ValueError):
            Synthetic(select_by="p150")
        Synthetic(select_by="p10")
        values = np.array([[1.0, 5.0], [2.0, 6.0], [3.0, 7.0]])
        np.testing.assert_allclose(synthetic.statistic(values, "median"), [2, 6])
        np.testing.assert_allclose(synthetic.statistic(values, "min"), [1, 5])
        np.testing.assert_allclose(synthetic.statistic(values, "p50"), [2, 6])

    def test_family_on_the_command_line(self):
        from ziplime.sweep.tools import parse_family
        self.assertEqual(parse_family("stationary:20"), {"method": "stationary", "block": 20})
        self.assertEqual(parse_family("garch"), {"method": "garch"})
        with self.assertRaises(SweepError):
            parse_family("block:x")


class SyntheticSweepTests(SweepParityCase):
    """A path is a bundle like the real one; the sweep over the paths is the sweep on each."""

    GRID = {"fast": [3, 5, 8], "slow": [12, 20]}

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.listings = [await self.listing(s) for s in ("JNJ@XNYS", "KO@XNYS", "PG@XNYS")]
        self.real = self.bundle(self.listings, seed=7, gaps=("KO",))
        universe = {listing.symbol: listing for listing in self.listings}
        self.bars = synthetic.side_by_side(self.real.data, universe, START, self.calendar.tz)
        self.feats = synthetic.features(self.bars)
        self.pool = np.arange(1, self.bars.shape[0])

    def path_bundle(self, family: Family, rng):
        made = synthetic.generator(family, self.feats, self.pool, self.bars.start)
        return synthetic.as_bundle(self.real, self.bars,
                                   synthetic.build(self.bars, self.feats, *made.path(rng)))

    def test_a_path_rewrites_only_the_numbers_of_the_run(self):
        made = self.path_bundle(Family(method="stationary", block=5), np.random.default_rng(0))
        before, after = self.real.data, made.data
        self.assertEqual(before.columns, after.columns)
        self.assertEqual(before.schema, after.schema)
        self.assertTrue(before.select("date", "sid").equals(after.select("date", "sid")))
        start = datetime.datetime.combine(START, datetime.time.min, tzinfo=self.calendar.tz)
        warm = pl.col("date") < start
        self.assertTrue(before.filter(warm).equals(after.filter(warm)))
        run_before, run_after = before.filter(~warm), after.filter(~warm)
        self.assertFalse(np.allclose(run_before["close"].to_numpy(), run_after["close"].to_numpy()))
        np.testing.assert_array_equal(run_after["close"].to_numpy(), run_after["price"].to_numpy())
        self.assertTrue((run_after["high"] >= run_after["close"]).all())
        self.assertTrue((run_after["low"] <= run_after["open"]).all())

    async def test_the_engine_agrees_with_the_sweep_on_a_path(self):
        # On a synthetic path, as on real bars: every combination to the cent
        for family in (Family(method="stationary", block=5), Family(method="garch"),
                       Family(method="meboot")):
            bundle = self.path_bundle(family, np.random.default_rng(3))
            with self.subTest(family=family.label):
                await self.assert_parity("trend.py", {"fast": [3, 5], "slow": [12]}, bundle)

    @unittest.skipUnless(mlx_available(), f"{GPU} is not installed")
    async def test_sweep_on_paths_on_the_gpu(self):
        families = [{"method": "stationary", "block": 5}, {"method": "meboot"},
                    {"method": "garch", "drift": "zero"}]
        output = self.folder / "synthetic"
        table = await self.sweep("trend.py", self.GRID, self.real, backend=GPU,
                                 synthetic={"paths": 4, "seed": 5, "families": families},
                                 output_dir=str(output), refine_top=50)
        summary = table.attrs["synthetic"]
        self.assertEqual(len(summary), 6)
        self.assertEqual(sorted(summary.index), list(range(6)))
        for name in ("real sharpe", "real rank", "stationary(5) median", "meboot rank",
                     "garch, zero drift P(gain)", "all median", "all rank"):
            self.assertIn(name, summary.columns)
        best = table.attrs["synthetic_best"]
        self.assertEqual(summary.loc[best, "all rank"], 1)
        config = json.loads((output / "best_synthetic_config.json").read_text())
        self.assertEqual((config["fast"], config["slow"]),
                         (summary.loc[best, "fast"], summary.loc[best, "slow"]))
        paths = pl.read_parquet(output / "synthetic_paths.parquet")
        self.assertEqual(paths.height, 3 * 4 * 6)
        # Each path's row is the sweep of that path on its own -- here in float64 on the CPU
        seeds = np.random.SeedSequence(5).spawn(3)
        for k, family in enumerate(families):
            rng = np.random.default_rng(seeds[k].spawn(4)[2])
            label = Family.model_validate(family).label
            alone = (await self.sweep("trend.py", self.GRID,
                                      self.path_bundle(Family.model_validate(family), rng))).sort_index()
            found = paths.filter((pl.col("family") == label) & (pl.col("path") == 2)).sort("combo")
            with self.subTest(family=label):
                np.testing.assert_array_equal(found["fills"].to_numpy(), alone["fills"].to_numpy())
                np.testing.assert_allclose(found["final_value"].to_numpy(),
                                           alone["final_value"].to_numpy(), rtol=1e-6)
        # The real bars' sweep is the ordinary one
        plain = await self.sweep("trend.py", self.GRID, self.real, backend=GPU)
        np.testing.assert_allclose(table.sort_index()["final_value"],
                                   plain.sort_index()["final_value"], rtol=1e-6)

    async def test_a_split_is_refused(self):
        jnj = self.listings[0]
        with sqlite3.connect(self.db) as connection:
            connection.execute("INSERT INTO splits (asset_id, effective_date, ratio) VALUES (?, ?, ?)",
                               (jnj.asset.id, "2024-03-04", 2.0))
        await self.asset_service._asset_repository.engine.dispose()
        self.asset_service = get_asset_service(db_path=str(self.db))
        with self.assertRaises(SweepError) as raised:
            await self.sweep("trend.py", self.GRID, self.real,
                             synthetic={"paths": 2, "families": [{"method": "iid"}]})
        self.assertIn("split", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
