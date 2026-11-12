# Parameter sweeps

A sweep runs one strategy for every combination of its parameters at once — thousands to millions
of backtests in seconds — on an Apple GPU, an NVIDIA GPU, or across CPU cores. The strategy is the
same algorithm file `run_simulation` runs, unchanged. The best combinations are recomputed exactly,
and the best one is run through `run_simulation` and its trades compared with the sweep's.

This page is how to run one. [How a sweep works](sweep-internals.md) explains why an ordinary
`handle_data` can run on a GPU and what is computed where; [Sweep performance](sweep-performance.md)
has the measured speeds and how to run a sweep on a rented NVIDIA GPU; [GPUs and
architectures](gpu-architectures.md) has which GPUs a sweep runs on — Apple, NVIDIA, AMD — and the
same sweeps compared across them.

## Install

```bash
pip install "ziplime[mlx]"     # Apple silicon
pip install "ziplime[cuda]"    # NVIDIA, through CuPy; for MLX on NVIDIA see Sweep performance
pip install ziplime            # CPU only: backend "cpu" or "exact"
```

A strategy that calls TA-Lib or scikit-learn needs them installed (`pip install ta-lib
scikit-learn`). ziplime needs pandas 2.x.

## Quick start

```bash
ziplime sweep doctor                      # which backends this machine has
ziplime sweep init strategy.py            # writes sweep.json: the strategy's numbers around their defaults
ziplime sweep check sweep.json            # a few combinations: how the strategy converts, in seconds
ziplime sweep run sweep.json              # the sweep
```

`ziplime sweep sweep.json` (without `run`) still works.

## The strategy

An ordinary algorithm file: an `AlgorithmConfig`, `initialize` and `handle_data`. The sweep varies
the fields of `AlgorithmConfig`.

```python
import math

import polars as pl

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 60


class AlgorithmConfig(BaseAlgorithmConfig):
    fast: int = 5
    slow: int = 20
    rebalance_every: int = 5
    invested: float = 0.95
    symbols: list[str] = ["SBER@MISX", "GAZP@MISX"]


async def initialize(context):
    config = context.algorithm.config
    context.universe = {s.split("@")[0]: await context.symbol(s) for s in config.symbols}
    context.i = 0
    price = pl.col("close").forward_fill()
    context.indicators(
        close=pl.col("close"),
        fast=price.rolling_mean(config.fast),
        slow=price.rolling_mean(config.slow),
    )


async def handle_data(context, data):
    bar = context.i
    context.i += 1
    config = context.algorithm.config
    if not context.signals.is_ready("slow") or bar % config.rebalance_every != 0:
        return
    weight = config.invested / len(context.universe)
    for name, asset in context.universe.items():
        if math.isnan(context.signals["close"][name]):
            continue
        if context.signals["fast"][name] > context.signals["slow"][name]:
            await context.order_target_percent(asset, weight, style=MarketOrder())
        elif await context.portfolio.get_asset_positions_amount(asset) > 0:
            await context.order_target_percent(asset, 0, style=MarketOrder())
```

Indicators can be declared in `initialize` as polars expressions with `context.indicators(...)`;
the engine and the sweep compute them once over the whole history and `handle_data` reads them as
`context.signals`, exactly as with `compute_signals` (which remains supported). An expression is
computed per instrument; one with its own `.over("date")` across instruments:

```python
context.indicators(
    momentum=pl.col("close").pct_change(config.lookback),
    rank=pl.col("momentum").rank(descending=True).over("date"),
)
```

`handle_data` may equally compute from history itself — `data.history(...)["close"].to_numpy()`
with NumPy, TA-Lib or a model fitted in `initialize`; the sweep computes that ahead of the run (see
[Computations on history](sweep-internals.md#computations-on-history)).

## The sweep file

```json
{
  "algorithm_file": "strategies/trend.py",
  "bundle": "my_bundle",
  "start_date": "2025-04-07",
  "end_date": "2025-12-30",
  "trading_calendar": "XMOS",
  "total_cash": 1000000,
  "equity_commission": {"type": "per_dollar", "cost": 0.0005},
  "equity_slippage": {"type": "fixed_bps", "basis_points": 5, "volume_limit": 0.1},
  "timing": "next_close",

  "grid": {
    "fast": {"from": 2, "to": 30},
    "slow": {"from": 10, "to": 60, "step": 2},
    "rebalance_every": [1, 2, 5, 10]
  },
  "where": "fast < slow",

  "backend": "mlx",
  "sort_by": "sharpe",
  "min_fills": 5,
  "verify": true,
  "output_dir": "results"
}
```

| Field | |
|---|---|
| `algorithm_file`, `config_file`, `config` | The strategy; a config file for its other fields; overrides of it shared by every combination |
| `grid` | Values of `AlgorithmConfig` fields: a list, `{"from", "to", "step"}`, or `{"path": "a.0.b", "values": [...]}` for a nested field |
| `where` | A Python condition over the grid's names that every combination must pass |
| `start_date`, `end_date`, `trading_calendar`, `emission_rate`, `total_cash` | As for run_simulation; `emission_rate` `1d`, or `1m`, `5m`, `1h` on a bundle of that frequency |
| `equity_commission`, `equity_slippage` | `{"type": "per_share" \| "per_dollar" \| "per_trade", "cost": ...}`; `{"type": "fixed_bps", "basis_points": 5, "volume_limit": 0.1}` or `fixed_spread` |
| `future_commission`, `future_slippage` | For futures in the universe; `{"type": "per_contract", "cost": 0.85, "exchange_fee": 1.5}` |
| `timing` | `same_close` (run_simulation's `same_bar_execution=True`), `next_close`, `next_open` |
| `backend` | `mlx` (Apple GPU; NVIDIA with `mlx[cuda]`), `cupy` (NVIDIA; AMD with CuPy built for ROCm), `dpnp` (Intel Arc), `opencl` (any OpenCL GPU — Intel Arc, AMD — for strategies that return entries/exits only), `cpu` (NumPy across `workers` processes), `exact` (NumPy, one process) |
| `compile` | Compile each bar into a few GPU kernels (MLX); on by default |
| `batch_size` | Combinations per GPU call; `auto` sizes it from the number of instruments |
| `sort_by`, `top`, `min_fills` | Ranking: `sharpe`, `total_return`, `final_value` or `max_drawdown`; rows shown; fewest fills to be ranked |
| `refine_top` | How many of the best GPU results (float32) to recompute in float64 before the final ranking; 50 |
| `verify` | Run the best combination through run_simulation and compare the trades |
| `synthetic` | Also run every combination on synthetic price paths made from the real bars and compare the choices; see [Sweeps on synthetic data](synthetic-data.md) |
| `on_unsupported` | A strategy that cannot be converted: `engine` (every combination through run_simulation, the default) or `refuse` |
| `asset_db`, `bundle`, `bundle_version`, `bundle_storage` | As for the command line; without `bundle`, daily bars are downloaded from Yahoo and cached in `cache_dir` |
| `output_dir` | Where the results go |

## The command line

| Command | |
|---|---|
| `ziplime sweep run SWEEP_FILE` | The sweep. Options override the file: `--backend`, `--grid NAME=SPEC`, `--set FIELD=VALUE`, `--compile/--no-compile`, `--batch-size`, `--workers`, `-o/--output-dir`, `--refuse`, `--no-verify` |
| `ziplime sweep check SWEEP_FILE` | The grid's corners — the first and last value of each parameter, 16 combinations at most — on the CPU (`--backend` to change), with the report and the warnings; writes nothing next to the file. Exit code 1 if the strategy cannot be converted |
| `ziplime sweep init ALGORITHM_FILE` | A sweep file whose grid varies every number of `AlgorithmConfig` around its default; `-o`, `--bundle`, `--start-date`, `--end-date`, `-c/--trading-calendar`, `--force` |
| `ziplime sweep doctor` | Python, pandas, the MLX and CuPy backends and their GPUs, TA-Lib, scikit-learn, CPU cores |

`--grid` takes a list (`--grid fast=5,10,20`) or a range (`--grid slow=30:90:10`, from 30 to 90 by
10) and replaces that parameter in the file's grid. `--set` sets any field
(`--set total_cash=50000 --set "where=fast < slow"`); values are read as JSON where they parse.

## From Python

`sweep_simulation` takes run_simulation's arguments, with the same meanings and defaults, and a
grid:

```python
import datetime

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.sweep import sweep_simulation

common = dict(
    algorithm_file="strategies/trend.py",
    start_date=datetime.datetime(2024, 1, 2),
    end_date=datetime.datetime(2025, 12, 30),
    trading_calendar="XNYS",
    emission_rate=datetime.timedelta(days=1),
    total_cash=100_000,
    market_data_source=bundle,                 # the DataBundle run_simulation would read
    asset_service=get_asset_service(),
    same_bar_execution=False,                  # fills at the next bar's close
)

table = await sweep_simulation(
    **common,
    grid={"fast": [5, 10, 20], "slow": {"from": 30, "to": 90, "step": 10}},
    where="fast < slow",
    backend="mlx",
    output_dir="results",
)
print(table.head())                            # every combination's metrics, best first
print(table.attrs["verified"])                 # the best one's trades against run_simulation
```

Any field of the sweep file can be passed as a keyword (`where`, `backend`, `refine_top`,
`verify`, `min_fills`, `on_unsupported`...). The commission and slippage models are run_simulation's
own (`equity_commission=PerShare(...)`). `run_sweep(SweepParams(...), base_dir)` runs a sweep
file's fields as they are.

## Sweeps and run_simulation

A sweep and `run_simulation` run the same algorithm file and are meant to be used together:

1. **The sweep checks itself against run_simulation.** With `verify` (the default) the best
   combination is run through run_simulation and its trades compared with the sweep's:

    ```
    Best {'fast': 9, 'slow': 40, 'rebalance_every': 5}: exact recomputation -- 56 fills, final value 890,219.02
      trades agree: yes; largest portfolio value gap: $0.00
    ```

2. **The best combination is a run_simulation config file.** `output_dir/best_config.json` holds
   every field of `AlgorithmConfig` for it; run it like any backtest — for the full report,
   another period, or live trading:

    ```python
    result = await run_simulation(**common, config_file="results/best_config.json")
    ```

    ```bash
    ziplime run -f strategies/trend.py --config-file results/best_config.json \
        -b my_bundle -s SBER@MISX,GAZP@MISX -c XMOS --start-date 2025-04-07 --end-date 2025-12-30 \
        --next-bar-execution --fill-price close
    ```

    Give it the same costs and execution as the sweep had, or its numbers are another backtest's.

3. **What a sweep cannot convert runs through run_simulation.** Every combination then goes
   through run_simulation on the CPU's cores, and the sweep says why (`on_unsupported: engine`).

## What a sweep writes and prints

* `sweep_results.csv` — every combination: its parameters, Sharpe, total return, maximum drawdown,
  final value, fills, fees, and whether it was computed in float32 or float64;
* `best_config.json` — the best combination as a config file for run_simulation;
* `best_fills.csv` — the best combination's every fill, from the exact recomputation.

Next to the ranking the sweep prints how the strategy ran — the hooks converted, whether the bars
were compiled and why not, every computation on history with its line and how it was computed —
and where the time went. And two mistakes only a run shows:

* **a computation on history that is NaN at every bar** — `talib.RSI(p[-30:], timeperiod=30)` has
  no value on 30 rows, so every comparison with it is False and the branch it decides never runs;
* **a parameter that changes nothing** — every set of combinations that differ in it alone has the
  same fills and final value.

```
How the strategy runs in the sweep:
  handle_data: converted -- every combination at once, on the GPU (mlx)
  bars: compiled, several bars per call
  computations on history, done before the run (27):
    composer.py:60: (((history(SOXL, close, 250 bars)[-21:][-1] / ...[0]) - 1) * 100) -- once on the whole history
    composer.py:63: talib.RSI(history(TQQQ, close, 250 bars)[-30:], timeperiod=30)[-1] -- bar by bar
    ...
Warnings:
  composer.py:63: talib.RSI(history(TQQQ, close, 250 bars)[-30:], timeperiod=30)[-1] is NaN at every bar: ...
  soxl_rsi changes nothing: combinations that differ in it alone have the same fills and final value. ...
```

## Synthetic data

A sweep can also run the grid on many synthetic price paths made from the real bars: block
bootstrap, maximum entropy bootstrap (meboot), GARCH, GBM and others. You can choose the
combination on the paths and look at it on the real bars, or choose on the real bars and see how
the winner does on each family of paths. See [Sweeps on synthetic data](synthetic-data.md).

```bash
ziplime sweep run sweep.json --paths 100 --family stationary:20 --family meboot --family garch
```

## Precision

GPUs compute in float32. The top `refine_top` combinations are recomputed in float64 before the
final ranking, and the best one is verified against the engine; `sweep_results.csv` says which
rows are which (`precision`). See [Precision and verification](sweep-internals.md#precision-and-verification).
