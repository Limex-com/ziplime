"""The batch kernel: many signal sets in one call, on the exact backend and on the GPU.

Two different promises are pinned here. The exact backend is a loop over `simulate_signals` and
nothing else, so its summaries have to be that function's numbers to the last bit. The GPU backends
are a second spelling of the kernel's decisions -- one source, compiled for Metal, CUDA and
OpenCL -- so on the same inputs each has to take the same decisions -- every fill, rejection and
no-op, by count and by reason -- and land on the same account values to float32 precision.

The Metal tests run only on an Apple silicon Mac with `mlx`, the CUDA tests only on an NVIDIA GPU
with CuPy, the OpenCL tests on any GPU with PyOpenCL and an OpenCL driver; each class is skipped
elsewhere. The rest run everywhere.
"""
import unittest
from unittest import mock

import numpy as np
import pandas as pd

from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.vectorized.kernel import (
    ExecutionTiming, VectorKernelError, simulate_signals, simulate_signals_batch,
)
from ziplime.vectorized.kernel import _cupy, _gpu, _mlx, _opencl
from ziplime.vectorized.kernel.batch import REJECT_REASONS

TZ = "America/New_York"


def bars(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-02", periods=n, freq="D", tz=TZ)


def market(seed: int, n_bars: int = 60, n_assets: int = 4):
    """Random-walk closes, opens near the previous close, and integer volumes. Seeded: a test
    that fails on some seeds is not a test."""
    rng = np.random.default_rng(seed)
    index = bars(n_bars)
    columns = [f"S{i}" for i in range(n_assets)]
    closes = pd.DataFrame(20.0 + np.abs(np.cumsum(rng.normal(0, 0.7, (n_bars, n_assets)), axis=0)),
                          index=index, columns=columns)
    opens = closes.shift(1).fillna(closes.iloc[0]) * (1 + rng.normal(0, 0.003, closes.shape))
    volumes = pd.DataFrame(rng.integers(0, 400, closes.shape).astype(float), index=index,
                           columns=columns)
    return rng, closes, opens, volumes


def stack(rng, n_combos: int, closes: pd.DataFrame, density: float) -> np.ndarray:
    return rng.random((n_combos, *closes.shape)) < density


class ExactBackendTests(unittest.TestCase):
    """The exact backend is `simulate_signals` in a loop, and its summaries say so exactly."""

    def setUp(self):
        rng, self.closes, _, _ = market(seed=11)
        self.entries = stack(rng, 6, self.closes, 0.2)
        self.exits = stack(rng, 6, self.closes, 0.2)
        self.kwargs = dict(size=50, initial_cash=5_000.0,
                           commission=PerShare(cost=0.005, min_trade_cost=1.0),
                           execution=ExecutionTiming.same_close())

    def single(self, k: int):
        frame = lambda array: pd.DataFrame(array, index=self.closes.index,
                                           columns=self.closes.columns)
        return simulate_signals(prices=self.closes, entries=frame(self.entries[k]),
                                exits=frame(self.exits[k]), **self.kwargs)

    def test_every_summary_is_the_single_run_number(self):
        batch = simulate_signals_batch(self.closes, self.entries, self.exits, **self.kwargs)
        self.assertEqual(batch.precision, "float64")
        for k in range(len(batch)):
            with self.subTest(combination=k):
                single = self.single(k)
                np.testing.assert_array_equal(batch.portfolio_value[k].to_numpy(),
                                              single.portfolio_value.to_numpy())
                self.assertEqual(batch.fills[k], len(single.fills))
                self.assertEqual(batch.no_ops[k], single.no_ops)
                self.assertEqual(batch.fees[k], sum(f.commission for f in single.fills))
                self.assertEqual(batch.cash[k], float(single.cash.iloc[-1]))
                self.assertEqual(int(batch.rejects.loc[k].sum()), len(single.rejects))

    def test_rerun_is_simulate_signals_on_that_combination(self):
        batch = simulate_signals_batch(self.closes, self.entries, self.exits, **self.kwargs)
        rerun, single = batch.rerun(3), self.single(3)
        self.assertEqual([(f.instrument, f.timestamp, f.amount, f.price) for f in rerun.fills],
                         [(f.instrument, f.timestamp, f.amount, f.price) for f in single.fills])

    def test_labels_index_every_result_and_name_the_rerun(self):
        labels = [(fast, slow) for fast in (5, 10) for slow in (20, 40, 60)]
        batch = simulate_signals_batch(self.closes, self.entries, self.exits, labels=labels,
                                       **self.kwargs)
        best = batch.final_value.idxmax()
        self.assertIn(best, labels)
        self.assertEqual(float(batch.rerun(best).portfolio_value.iloc[-1]),
                         float(batch.final_value[best]))
        with self.assertRaises(KeyError):
            batch.rerun((7, 7))

    def test_a_list_of_frames_is_the_same_as_a_stack(self):
        frames = [pd.DataFrame(array, index=self.closes.index, columns=self.closes.columns)
                  for array in self.entries]
        exits = [pd.DataFrame(array, index=self.closes.index, columns=self.closes.columns)
                 for array in self.exits]
        from_frames = simulate_signals_batch(self.closes, frames, exits, **self.kwargs)
        from_stack = simulate_signals_batch(self.closes, self.entries, self.exits, **self.kwargs)
        pd.testing.assert_frame_equal(from_frames.portfolio_value, from_stack.portfolio_value)

    def test_nan_in_a_float_signal_is_no_signal(self):
        """`fast > slow` computed by hand in floats carries NaN through the warm-up, and NaN is
        truthy to NumPy. Reading it as a signal would trade every warm-up bar."""
        as_floats = np.where(self.entries, 1.0, np.nan)
        from_floats = simulate_signals_batch(self.closes, as_floats, self.exits, **self.kwargs)
        from_bools = simulate_signals_batch(self.closes, self.entries, self.exits, **self.kwargs)
        pd.testing.assert_series_equal(from_floats.fills, from_bools.fills)

    def test_a_signal_on_the_last_bar_is_counted_as_having_nowhere_to_fill(self):
        entries = np.zeros((2, *self.closes.shape), dtype=bool)
        entries[1, -1, 0] = True
        batch = simulate_signals_batch(self.closes, entries, size=10,
                                       execution=ExecutionTiming.next_close())
        self.assertEqual(list(batch.rejects["no_execution_bar"]), [0, 1])


class RefusalTests(unittest.TestCase):
    """What the single-run path refuses, the batch refuses -- plus the shapes only it can get
    wrong."""

    def setUp(self):
        rng, self.closes, _, _ = market(seed=2, n_bars=10, n_assets=2)
        self.entries = stack(rng, 3, self.closes, 0.3)

    def refused(self, **kwargs) -> str:
        kwargs.setdefault("entries", self.entries)
        with self.assertRaises(VectorKernelError) as caught:
            simulate_signals_batch(self.closes, **kwargs)
        return str(caught.exception)

    def test_stacks_disagreeing_about_the_number_of_combinations(self):
        self.assertIn("disagree", self.refused(exits=self.entries[:2]))

    def test_a_stack_whose_slices_are_not_shaped_like_prices(self):
        self.assertIn("expected (combinations, 10, 2)", self.refused(entries=self.entries[:, :5]))

    def test_a_two_dimensional_array_is_pointed_at_the_missing_axis(self):
        self.assertIn("array[None]", self.refused(entries=self.entries[0]))

    def test_no_signals_at_all(self):
        self.assertIn("nothing to sweep", self.refused(entries=None))

    def test_shorts_without_permission(self):
        self.assertIn("allow_short", self.refused(short_entries=self.entries))

    def test_repeated_labels(self):
        self.assertIn("repeat", self.refused(labels=["a", "a", "b"]))

    def test_a_label_per_combination(self):
        self.assertIn("one each", self.refused(labels=["a", "b"]))

    def test_an_unknown_backend(self):
        message = self.refused(backend="cuda")
        self.assertIn("not a backend", message)
        self.assertIn("'cupy'", message)

    def test_a_cost_model_the_kernel_cannot_price(self):
        from ziplime.finance.slippage.volume_share_slippage import VolumeShareSlippage

        self.assertIn("VolumeShareSlippage", self.refused(slippage=VolumeShareSlippage()))

    def test_next_open_without_opens(self):
        self.assertIn("needs open prices", self.refused(execution=ExecutionTiming.next_open()))

    def test_the_gpu_without_mlx_is_refused_rather_than_replaced(self):
        """Falling back to the exact backend would change the numbers without saying so."""
        with mock.patch.object(_mlx, "HAVE_MLX", False):
            message = self.refused(backend="mlx")
        self.assertIn("pip install mlx", message)
        self.assertIn("backend='cupy'", message)
        self.assertIn("backend='exact'", message)

    def test_mlx_without_metal_points_at_cupy(self):
        """MLX on Linux (mlx[cuda]) imports fine and has no Metal to run this kernel on."""
        with mock.patch.object(_mlx, "HAVE_MLX", True), \
                mock.patch.object(_mlx, "_has_metal", return_value=False):
            message = self.refused(backend="mlx")
        self.assertIn("no Metal device", message)
        self.assertIn("backend='cupy'", message)

    def test_cuda_without_cupy_is_refused_rather_than_replaced(self):
        with mock.patch.object(_cupy, "HAVE_CUPY", False):
            message = self.refused(backend="cupy")
        self.assertIn("ziplime[cuda]", message)
        self.assertIn("backend='exact'", message)

    def test_cupy_without_a_device(self):
        with mock.patch.object(_cupy, "HAVE_CUPY", True), \
                mock.patch.object(_cupy, "_device_count", return_value=0), \
                mock.patch.object(_cupy, "is_rocm", return_value=False):
            self.assertIn("no CUDA device", self.refused(backend="cupy"))

    def test_cupy_for_rocm_without_a_device_points_at_rocm(self):
        with mock.patch.object(_cupy, "HAVE_CUPY", True), \
                mock.patch.object(_cupy, "_device_count", return_value=0), \
                mock.patch.object(_cupy, "is_rocm", return_value=True):
            self.assertIn("rocm-smi", self.refused(backend="cupy"))

    def test_opencl_without_pyopencl_is_refused_rather_than_replaced(self):
        with mock.patch.object(_opencl, "HAVE_PYOPENCL", False):
            message = self.refused(backend="opencl")
        self.assertIn("ziplime[opencl]", message)
        self.assertIn("backend='exact'", message)

    def test_opencl_without_a_gpu(self):
        with mock.patch.object(_opencl, "HAVE_PYOPENCL", True), \
                mock.patch.object(_opencl, "_gpu_devices", return_value=[]), \
                mock.patch.dict("os.environ", {"PYOPENCL_CTX": ""}):
            self.assertIn("no OpenCL GPU", self.refused(backend="opencl"))


class GpuAgreesWithExact:
    """A GPU kernel against the reference, decision for decision.

    Each scenario turns on the features that take a different branch in the loop. Counts have to
    match exactly: a decision is a decision. Values have to match to float32 precision, which on
    these account sizes is well under a cent per thousand.
    """

    def both(self, closes, **kwargs):
        exact = simulate_signals_batch(closes, backend="exact", **kwargs)
        gpu = simulate_signals_batch(closes, backend=self.backend, **kwargs)
        self.assertEqual(gpu.precision, "float32")
        return exact, gpu

    def assert_agree(self, exact, gpu):
        pd.testing.assert_series_equal(gpu.fills, exact.fills)
        pd.testing.assert_series_equal(gpu.no_ops, exact.no_ops)
        pd.testing.assert_frame_equal(gpu.rejects, exact.rejects)
        np.testing.assert_allclose(gpu.positions.to_numpy(), exact.positions.to_numpy(),
                                   rtol=1e-6, atol=1e-4)
        np.testing.assert_allclose(gpu.portfolio_value.to_numpy(),
                                   exact.portfolio_value.to_numpy(), rtol=2e-6, atol=1e-2)
        np.testing.assert_allclose(gpu.fees.to_numpy(), exact.fees.to_numpy(),
                                   rtol=1e-5, atol=1e-3)

    def test_costs_and_a_cash_constraint_that_binds(self):
        rng, closes, _, _ = market(seed=5)
        exact, gpu = self.both(
            closes, entries=stack(rng, 24, closes, 0.2), exits=stack(rng, 24, closes, 0.2),
            size=50, initial_cash=3_000.0, commission=PerShare(cost=0.005, min_trade_cost=1.0),
            slippage=FixedBasisPointsSlippage(basis_points=5.0),
            execution=ExecutionTiming.same_close())
        self.assertGreater(int(exact.rejects["insufficient_cash"].sum()), 0)
        self.assert_agree(exact, gpu)

    def test_shorts_reversals_and_accumulation(self):
        rng, closes, _, _ = market(seed=8)
        for accumulate in (False, True):
            with self.subTest(accumulate=accumulate):
                exact, gpu = self.both(
                    closes, entries=stack(rng, 16, closes, 0.15),
                    exits=stack(rng, 16, closes, 0.15),
                    short_entries=stack(rng, 16, closes, 0.15),
                    short_exits=stack(rng, 16, closes, 0.15),
                    allow_short=True, accumulate=accumulate, size=20, initial_cash=50_000.0,
                    commission=PerTrade(cost=1.0), execution=ExecutionTiming.next_close())
                self.assertGreater(int(exact.rejects["nothing_to_close"].sum()), 0)
                self.assert_agree(exact, gpu)

    def test_nan_prices_and_nan_sizes(self):
        rng, closes, _, _ = market(seed=13)
        closes = closes.mask(rng.random(closes.shape) < 0.08)
        sizes = pd.DataFrame(25.0, index=closes.index, columns=closes.columns)
        sizes = sizes.mask(rng.random(sizes.shape) < 0.08)
        exact, gpu = self.both(
            closes, entries=stack(rng, 16, closes, 0.25), exits=stack(rng, 16, closes, 0.2),
            size=sizes, initial_cash=20_000.0, slippage=FixedSlippage(spread=0.02),
            execution=ExecutionTiming.same_close())
        self.assertGreater(int(exact.rejects["nan_price"].sum()), 0)
        self.assertGreater(int(exact.rejects["nan_size"].sum()), 0)
        self.assert_agree(exact, gpu)

    def capped(self, seed: int, volumes_in_tens: bool):
        rng, closes, opens, volumes = market(seed=seed)
        if volumes_in_tens:
            volumes = volumes // 10 * 10
        # Some bars trade nothing, so the cap leaves no room; some carry no volume at all, which
        # caps nothing.
        volumes = volumes.where(rng.random(volumes.shape) > 0.1, 0.0)
        volumes = volumes.mask(rng.random(volumes.shape) < 0.05)
        return self.both(
            closes, entries=stack(rng, 16, closes, 0.2), exits=stack(rng, 16, closes, 0.2),
            opens=opens, volumes=volumes, size=60, initial_cash=30_000.0,
            commission=PerDollar(cost=0.001),
            slippage=FixedBasisPointsSlippage(basis_points=10.0, volume_limit=0.1),
            execution=ExecutionTiming.next_open())

    def test_next_open_with_a_volume_cap(self):
        """Volumes in tens, so a 10% cap leaves whole units and both paths hold exactly the same
        positions -- see the next test for what happens when it does not."""
        exact, gpu = self.capped(seed=21, volumes_in_tens=True)
        self.assertGreater(int(exact.rejects["zero_size"].sum()), 0)
        self.assertGreater(int(exact.rejects["no_execution_bar"].sum()), 0)
        self.assert_agree(exact, gpu)

    def test_fractional_positions_can_differ_by_dust_and_by_nothing_else(self):
        """The one way the two paths count fills differently, pinned so it stays the only one.

        A 10% cap on 23 units of volume buys 2.3 units, which neither float type holds exactly.
        Sold off in capped pieces, the position can end at about 1e-15 in float64 and at exactly
        zero in float32. The exact kernel then closes that residue as one more fill -- of no
        economic size -- where the GPU sees a flat book and records a no-op. Values agree.
        """
        exact, gpu = self.capped(seed=21, volumes_in_tens=False)
        differs = exact.fills != gpu.fills
        self.assertTrue(differs.any(), "the scenario no longer produces a residue to compare")
        for label in exact.fills.index[differs]:
            dust = sum(abs(fill.amount) < 1e-9 for fill in exact.rerun(label).fills)
            self.assertLessEqual(abs(int(exact.fills[label]) - int(gpu.fills[label])), dust)
        np.testing.assert_allclose(gpu.portfolio_value.to_numpy(),
                                   exact.portfolio_value.to_numpy(), rtol=2e-6, atol=1e-2)

    def test_more_combinations_than_one_threadgroup(self):
        """Several threadgroups, and a count that is not a multiple of one, so the last group
        runs part-full."""
        rng, closes, _, _ = market(seed=34, n_bars=40, n_assets=3)
        exact, gpu = self.both(
            closes, entries=stack(rng, 3 * _gpu.THREADGROUP + 5, closes, 0.2),
            exits=stack(rng, 3 * _gpu.THREADGROUP + 5, closes, 0.2), size=30,
            initial_cash=2_500.0, execution=ExecutionTiming.next_close())
        self.assert_agree(exact, gpu)

    def test_the_rerun_of_a_gpu_winner_is_the_float64_run(self):
        rng, closes, _, _ = market(seed=55)
        gpu = simulate_signals_batch(
            closes, stack(rng, 10, closes, 0.2), stack(rng, 10, closes, 0.2), size=40,
            initial_cash=10_000.0, execution=ExecutionTiming.same_close(), backend=self.backend)
        best = gpu.final_value.idxmax()
        exact = gpu.rerun(best)
        self.assertEqual(len(exact.fills), gpu.fills[best])
        self.assertAlmostEqual(float(exact.portfolio_value.iloc[-1]),
                               float(gpu.final_value[best]), delta=0.05)

    def test_the_reject_columns_are_the_documented_reasons(self):
        rng, closes, _, _ = market(seed=1, n_bars=10, n_assets=2)
        gpu = simulate_signals_batch(closes, stack(rng, 2, closes, 0.3), size=1,
                                     execution=ExecutionTiming.same_close(),
                                     backend=self.backend)
        self.assertEqual(list(gpu.rejects.columns), list(REJECT_REASONS))


@unittest.skipUnless(_mlx.HAVE_MLX and _mlx._has_metal(),
                     "no Metal: the mlx backend needs an Apple silicon Mac with mlx installed")
class MetalAgreesWithExactTests(GpuAgreesWithExact, unittest.TestCase):
    backend = "mlx"


@unittest.skipUnless(_cupy.HAVE_CUPY and _cupy._device_count(),
                     "no CuPy GPU: the cupy backend needs CuPy and an NVIDIA (CUDA) or AMD (ROCm) "
                     "GPU")
class CudaAgreesWithExactTests(GpuAgreesWithExact, unittest.TestCase):
    backend = "cupy"


@unittest.skipUnless(_opencl.HAVE_PYOPENCL and _opencl._gpu_devices(),
                     "no OpenCL GPU: the opencl backend needs PyOpenCL and a GPU's OpenCL driver")
class OpenClAgreesWithExactTests(GpuAgreesWithExact, unittest.TestCase):
    backend = "opencl"


if __name__ == "__main__":
    unittest.main()
