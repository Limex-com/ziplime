"""A parameter sweep on the GPU: every combination at once, the winner rerun exactly.

`sweep_then_report.py` tries 48 moving-average pairs one after another. This tries a couple of
thousand over fifty instruments -- a synthetic universe, so it runs offline -- through
`simulate_signals_batch`, once on each backend:

* `exact`: the float64 kernel, one combination at a time. Timed on a sample and extrapolated,
  because the whole grid takes minutes this way.
* `mlx`: every combination at once on the Apple GPU, one thread each, in float32.

Then it holds the two against each other on the sample, and reruns the GPU's winner through the
float64 kernel. That rerun, not the GPU's number, is what `to_execution_result` replays into a
ziplime report -- `sweep_then_report.py` shows that half.

Run:

    python -m examples.vectorized.sweep_on_gpu

Needs `pip install mlx`, which exists for Apple silicon only.
"""
import itertools
import logging
import time

import numpy as np
import pandas as pd
import structlog

from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals_batch

N_INSTRUMENTS = 50
N_SESSIONS = 1000
SEED = 7

#: The grid: every fast window below every slow one, about 2,700 pairs.
FAST_WINDOWS = range(2, 101, 2)
SLOW_WINDOWS = range(10, 301, 5)

#: Units per signal, and enough cash for most of the book to be held at once.
SIZE = 100
CASH = 1_000_000.0

#: Combinations the exact backend is timed on. The rest is extrapolated from them.
SAMPLE = 20


def synthetic_closes() -> pd.DataFrame:
    """Geometric random walks with their own drift and volatility, one per instrument."""
    rng = np.random.default_rng(SEED)
    index = pd.bdate_range("2021-01-04", periods=N_SESSIONS, tz="America/New_York")
    drift = rng.normal(0.0003, 0.0004, N_INSTRUMENTS)
    volatility = rng.uniform(0.01, 0.03, N_INSTRUMENTS)
    returns = rng.normal(drift, volatility, (N_SESSIONS, N_INSTRUMENTS))
    closes = rng.uniform(20.0, 200.0, N_INSTRUMENTS) * np.exp(np.cumsum(returns, axis=0))
    return pd.DataFrame(closes, index=index,
                        columns=[f"SYN{k:02d}" for k in range(N_INSTRUMENTS)])


def signal_grid(closes: pd.DataFrame, grid: list[tuple[int, int]]):
    """Level signals for every pair, stacked ``(combinations, sessions, instruments)``.

    Each window's trailing mean is computed once and shared by every pair that uses it -- the
    grid has 2,700 pairs but only 108 windows. NaN through the warm-up compares False, so no pair
    trades before its slow window fills.
    """
    values = closes.to_numpy()
    running = np.vstack([np.zeros((1, values.shape[1])), np.cumsum(values, axis=0)])
    means = {}
    for window in sorted({window for pair in grid for window in pair}):
        mean = np.full(values.shape, np.nan)
        mean[window - 1:] = (running[window:] - running[:-window]) / window
        means[window] = mean

    entries = np.empty((len(grid), *values.shape), dtype=bool)
    exits = np.empty_like(entries)
    for k, (fast, slow) in enumerate(grid):
        np.greater(means[fast], means[slow], out=entries[k])
        np.less(means[fast], means[slow], out=exits[k])
    return entries, exits


def main():
    # Every exact run logs a line; twenty of them would bury the comparison this prints.
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))

    closes = synthetic_closes()
    grid = [(fast, slow) for fast, slow in itertools.product(FAST_WINDOWS, SLOW_WINDOWS)
            if fast < slow]
    started = time.perf_counter()
    entries, exits = signal_grid(closes, grid)
    print(f"Signals: {len(grid):,} combinations x {N_SESSIONS:,} sessions x {N_INSTRUMENTS} "
          f"instruments, built in {time.perf_counter() - started:.1f}s")

    shared = dict(prices=closes, size=SIZE, initial_cash=CASH,
                  execution=ExecutionTiming.next_close(),
                  commission=PerShare(cost=0.005, min_trade_cost=1.0),
                  slippage=FixedBasisPointsSlippage(basis_points=5.0))

    # The first GPU call compiles the Metal kernel. Paid here, so it is not reported as speed.
    simulate_signals_batch(entries=entries[:1], exits=exits[:1], backend="mlx", **shared)

    started = time.perf_counter()
    gpu = simulate_signals_batch(entries=entries, exits=exits, labels=grid, backend="mlx",
                                 **shared)
    gpu_seconds = time.perf_counter() - started

    sample = np.linspace(0, len(grid) - 1, SAMPLE).astype(int)
    sample_labels = [grid[k] for k in sample]
    started = time.perf_counter()
    exact = simulate_signals_batch(entries=entries[sample], exits=exits[sample],
                                   labels=sample_labels, backend="exact", **shared)
    exact_each = (time.perf_counter() - started) / SAMPLE

    print(f"\nGPU (mlx):    {len(grid):,} combinations in {gpu_seconds:.2f}s "
          f"({gpu_seconds / len(grid) * 1000:.2f} ms each)")
    print(f"CPU (exact):  {exact_each * 1000:.0f} ms each, measured on {SAMPLE}; the whole grid "
          f"would take about {exact_each * len(grid):.0f}s")
    print(f"              the GPU is about {exact_each * len(grid) / gpu_seconds:.0f}x faster "
          f"on this grid")

    same_fills = bool((gpu.fills[sample_labels] == exact.fills).all())
    gap = float((gpu.final_value[sample_labels] - exact.final_value).abs().max())
    print(f"\nOn the {SAMPLE} sampled combinations: fills identical: {same_fills}; largest "
          f"difference in ending value: ${gap:.2f} on about ${CASH:,.0f}")

    ranked = gpu.final_value.sort_values(ascending=False)
    print("\nBest five by ending value (the GPU's float32 numbers, for ranking):")
    for fast, slow in ranked.index[:5]:
        print(f"   SMA {fast:>3}/{slow:<3}  ending {gpu.final_value[(fast, slow)]:>14,.2f}  "
              f"return {gpu.total_return[(fast, slow)]:+7.2%}  "
              f"fills {gpu.fills[(fast, slow)]:>6,}  fees {gpu.fees[(fast, slow)]:>9,.2f}")

    best = ranked.index[0]
    rerun = gpu.rerun(best)
    print(f"\nRerun of SMA {best[0]}/{best[1]} on the float64 kernel: "
          f"{len(rerun.fills):,} fills, ending {float(rerun.portfolio_value.iloc[-1]):,.2f} "
          f"(the GPU said {gpu.final_value[best]:,.2f}). This result, with every fill, is what "
          f"to_execution_result replays into a ziplime report.")
    print("\nThe winner is picked over the whole window, which is pure overfitting: the numbers "
          "describe the sweep, not a strategy.")


if __name__ == "__main__":
    main()
