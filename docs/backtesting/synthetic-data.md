# Sweeps on synthetic data

A sweep picks the combination that did best on one history: the one that happened. A sweep on
synthetic data runs the same grid again on many price paths that could have happened instead,
made from the real bars. That tells you which choices hold up:

* **Choose on the paths, look at the real bars.** The combination that is best across all paths
  (by the median Sharpe ratio, say) is a choice that does not depend on one history. Its result
  on the real bars, and its rank there, show what that robustness costs.
* **Choose on the real bars, look at the paths.** The real winner's distribution over each family
  of paths shows whether it wins on histories like the real one, or only on the real one.

Every combination runs on every path the same way it runs on the real bars: the same converted
strategy, the same costs and execution, the same backend. Every combination of a path goes in one
GPU call.

## Quick start

Add `synthetic` to a sweep file:

```json
{
  "algorithm_file": "strategies/rsi_reversion.py",
  "start_date": "2016-01-04",
  "end_date": "2026-10-30",
  "grid": {"period": [7, 14, 21, 28], "lower": {"from": 10, "to": 40, "step": 2},
           "upper": {"from": 50, "to": 90, "step": 2}},
  "backend": "mlx",
  "min_fills": 50,

  "synthetic": {
    "paths": 100,
    "families": [
      {"method": "stationary", "block": 20},
      {"method": "meboot"},
      {"method": "garch"}
    ]
  }
}
```

Or add `synthetic` from the command line to any sweep file:

```bash
ziplime sweep run sweep.json --paths 100 --family stationary:20 --family meboot --family garch
```

From Python, `sweep_simulation(..., synthetic={"paths": 100, "families": [...]})` takes the same
fields.

The sweep on the real bars runs first, unchanged: the ranking, the float64 refinement, and the
check of the best combination against run_simulation. Then each family's paths run, and the two
sides are compared:

```
Synthetic paths: 3 families × 5 paths of 2723 bars, from 2798 real bars (history); every combination on each
  stationary(20): 5 paths in 12.6 s
  meboot: 5 paths in 12.6 s
  garch: 5 paths in 12.4 s

Chosen on the synthetic paths (median sharpe over all 15 paths): {'period': 28, 'lower': 38, 'upper': 82}
  family           median       p5      p95  P(gain)   rank
  stationary(20)     1.02     0.84     1.32     100%   93 of 1,026 (top 10%)
  meboot             0.83     0.78     0.86     100%   3 of 989 (top 1%)
  garch              0.51     0.25     0.70     100%   11 of 1,102 (top 1%)
  real bars          0.82                              21 of 983 (top 3%)

Chosen on the real bars: {'period': 28, 'lower': 30, 'upper': 82} -- sharpe 0.87
  family           median       p5      p95  P(gain)   rank                        real beats
  stationary(20)     0.95     0.58     1.30     100%   165 of 1,026 (top 17%)    20% of its paths
  meboot             0.83     0.70     0.89     100%   2 of 989 (top 1%)         80% of its paths
  garch              0.42    -0.01     0.49      80%   41 of 1,102 (top 4%)      100% of its paths

Best on each family, and on the real bars:
  stationary(20) {'period': 14, 'lower': 30, 'upper': 90}: median 1.21; real 0.83, 13 of 983 (top 2%)
  meboot         {'period': 28, 'lower': 40, 'upper': 82}: median 0.84; real 0.84, 11 of 983 (top 2%)
  garch          {'period': 28, 'lower': 36, 'upper': 82}: median 0.56; real 0.82, 23 of 983 (top 3%)

Rankings agree (Spearman, real sharpe against median of the paths): stationary(20) +0.82 · meboot +0.89 · garch +0.32
```

How to read it:

* **median, p5, p95** are the ranked metric (`sort_by`) over the combination's paths in that
  family; **P(gain)** is the share of paths that ended with a gain.
* **rank** is the combination's place in the family by `select_by` (the median by default),
  among the combinations that made `min_fills` fills on the median path. Each family can have a
  different number of such combinations.
* **real beats N% of its paths** is where the real result falls among the combination's own
  paths. Near 100% means the real history was unusually kind to this combination: it was lucky
  rather than skilful, at least by that family's idea of what could have happened.
* **Rankings agree** is the rank correlation, over all combinations, between the real ranking
  and the family's. High: the family keeps what the strategy lives on, and the paths rank the
  grid as reality does. Near zero: on that family the grid is noise.

## The families

A path is a whole set of bars (open, high, low, close and volume for every instrument) from
`start_date` on. The warm-up before `start_date` stays real, so every path starts where the real
run starts, with the same indicators. Each bar of a path is built from one real bar: its
close-to-close log return, its opening gap, its high and low relative to its close, and its
volume.

| `method` | How a path is made | What it keeps | What it breaks |
|---|---|---|---|
| `iid` | Real bars drawn at random, with replacement | Each day's returns across instruments, fat tails | Every dependence in time |
| `shuffle` | The real bars in a random order, without replacement | The same, and the multiset of returns: with `source: run` every path ends at the real final price | Every dependence in time |
| `block` | Blocks of `block` consecutive real bars (moving block bootstrap) | Trends, volatility clusters and mean reversion shorter than a block | Anything longer than a block |
| `circular` | The same, with blocks that wrap from the end to the start | The same, with every bar equally likely | The same |
| `stationary` | Blocks of random length, `block` bars on average (Politis & Romano) | The same, without a fixed block length the strategy could fit | The same |
| `meboot` | Each instrument's log price redrawn from the maximum entropy density around its own values, in the real order of their ranks (Vinod) | The real path's shape: its turns where they were, its highs and lows on their dates | Little: paths are close relatives of the real one |
| `gbm` | Normal log returns with the real means and covariance | Drift, volatility, correlation | Fat tails, volatility clusters, every dependence in time |
| `student_t` | The same with fat tails: a multivariate t with `df` degrees of freedom | Also fat tails | Volatility clusters, every dependence in time |
| `garch` | GARCH(1,1) per instrument, fitted by maximum likelihood and driven by the real standardised shocks of a random date (filtered historical simulation), starting from the volatility at `start_date` | Volatility clusters, fat tails, the correlation of the shocks | Trends and mean reversion in the returns themselves |

The resampling methods (`iid`, `shuffle`, `block`, `circular`, `stationary`) take the same real
date for every instrument, so the instruments move together as they did. `gbm`, `student_t` and
`garch` take the bar shape and the volume from a random real date as well; `meboot` takes them
from the bar's own date.

Families answer different questions, so use several:

* **Is it the structure?** `stationary` keeps what lasts less than a block. A strategy that also
  ranks well on it does not depend on the exact sequence of the real days.
* **Is it the luck of this history?** `meboot` paths stay close to the real one. A winner that
  falls apart on them was fitted to the details of one history.
* **Is it the market regime?** `garch` keeps volatility and its clustering but not trends. A
  trend strategy should do worse on it; one that does just as well is not trading trends.
* **Is it the drift?** `"drift": "zero"` takes each instrument's mean log return out of a family.
  A long-only strategy that earns only from the drift does no better than holding on such paths.

## Settings

| Field | | Default |
|---|---|---|
| `paths` | Paths per family | 100 |
| `seed` | The same seed gives the same paths | 0 |
| `families` | A list of `{"method": ...}` with the method's settings | `[{"method": "stationary"}]` |
| `families[].block` | `block`, `circular`: bars per block; `stationary`: the mean block length | 20 |
| `families[].drift` | `historical`, or `zero` to remove each instrument's mean log return | `historical` |
| `families[].df` | `student_t`: degrees of freedom, more than 2 | 4 |
| `families[].trim`, `expand_sd` | `meboot`: trimming of the mean absolute change that sets the tails; widen a replicate narrower than the data (R's `expand.sd`) | 0.10, true |
| `families[].name` | The family's name in the report and the files | the method and its settings |
| `source` | The bars a path is resampled or fitted from: `history`, everything loaded (warm-up included), or `run`, from `start_date` on | `history` |
| `select_by` | How a combination's paths are summed up to choose by: `median`, `mean`, `min`, or a percentile such as `p10` (the bad years) | `median` |

The command line's `--family METHOD[:BLOCK]` replaces the file's families and `--paths N` sets
`paths`. Other settings go through `--set synthetic='{...}'`.

## What it writes

Next to the sweep's own files in `output_dir`:

* `synthetic_summary.csv`: one row per combination. It has the parameters, the real metric and
  rank, and for every family the median, p5, p95, P(gain), rank, and where the real result falls
  among the combination's paths (`real percentile`). The `all` columns pool every family's paths.
* `synthetic_paths.parquet`: every combination on every path, in long format: `family`, `path`,
  `combo` and the metrics (`sharpe`, `total_return`, `max_drawdown`, `final_value`, `fills`).
  Read it with `pl.read_parquet` for your own statistics, such as the distribution of drawdowns,
  a probability of ruin, or a chart of one combination's paths.
* `best_synthetic_config.json`: the combination best across all paths, as a config file for
  run_simulation. `best_config.json` is still the real bars' best.

From Python, `table.attrs["synthetic"]` is the summary and `table.attrs["synthetic_best"]` the
combination chosen on the paths.

## Speed

The strategy is initialized, converted and checked once. Every path then costs one sweep of the
grid on the GPU plus what the strategy computes from prices on the CPU (indicators,
`compute_signals`, computations on history), since every path has new prices. For the RSI strategy
above (1,344 combinations, 12 instruments, 2,723 bars, an M-series GPU), a path takes 0.17 s on
the GPU and 2.3 s in the strategy's `compute_signals`. A strategy with polars indicators
(`context.indicators`) is faster per path.

The paths run in float32 on the GPU, like the sweep before its refinement. Their summary is a
statistic over many paths, which a cent's rounding does not move. Only the real bars' best
combination is refined in float64 and checked against run_simulation.

## Limits

* **Daily bars.** An intraday path would have to keep each session's minutes in order.
* **No splits in the run.** A split happens on its date in the asset database, but a path moves
  the split's price jump to another day. A sweep with a split in the run refuses synthetic paths;
  use bars adjusted for splits. Cash dividends are paid as in the real run, in the same amounts.
* **One path per run of the grid.** Paths are not yet stacked into one GPU call; a grid of a few
  hundred combinations leaves the GPU mostly idle on each path.
* **Event data and datasets stay real.** `context.datasets` (fundamentals, filings) are the real
  ones on every path.
