# Release Notes

### Version 2.12.0

Parameter sweeps: one strategy run for every combination of its parameters at once -- on an Apple
GPU, an NVIDIA GPU or across CPU cores -- from the ordinary algorithm file, with the best
combination checked against run_simulation. A million combinations take seconds to minutes. See
docs/backtesting/parameter-sweeps.md, how it works in docs/backtesting/sweep-internals.md and the
measured speeds in docs/backtesting/sweep-performance.md.

#### Upgrading from 2.11.0

- **Backtests are unchanged** except for the fixes below; a strategy that looks a position up by
  its asset now finds it (see *Backtest results that change*).
- **`ziplime sweep` is a group of commands.** `ziplime sweep sweep.json` still runs the file, as
  `ziplime sweep run sweep.json` does.
- **Extras.** `ziplime[mlx]` (Apple silicon) and `ziplime[cuda]` (NVIDIA, CuPy). MLX on NVIDIA:
  `mlx[cuda]` with `nvidia-cuda-runtime-cu12>=12.8`. ziplime needs pandas 2.x.

#### New

- **The signals kernel on NVIDIA.** `simulate_signals_batch(..., backend="cupy")` runs every
  combination at once on an NVIDIA GPU through CuPy (`ziplime[cuda]`), from the same kernel source
  as `backend="mlx"` on Apple silicon, and a sweep file with `"backend": "cupy"` whose strategy
  returns entries/exits now runs on it instead of being refused. `examples/vectorized/sweep_on_gpu.py`
  takes whichever GPU backend the machine has. `backend="mlx"` on MLX without Metal (`mlx[cuda]`)
  is refused up front with a pointer to `cupy`, where it used to fail inside MLX.
- **The signals kernel on Intel Arc and AMD.** `backend="opencl"` runs the same kernel source on any
  GPU with an OpenCL driver, through PyOpenCL (`ziplime[opencl]`), in `simulate_signals_batch` and
  in a sweep whose strategy returns entries/exits. A strategy swept through `handle_data` is refused
  on `opencl`: that path needs an array library on the GPU. `ziplime sweep doctor` lists the
  OpenCL GPU.
- **Intel Arc: `backend: dpnp`.** A strategy swept through `handle_data` runs on dpnp, Intel's
  NumPy for SYCL; one that returns entries/exits runs the vector kernel through PyOpenCL on the same
  GPU. Checked against exact on dpnp's CPU device; not yet on an Arc GPU. See "A sweep on an Intel
  GPU" in docs/backtesting/sweep-performance.md.
- **AMD through CuPy for ROCm.** `backend="cupy"` recognises a ROCm build of CuPy: the entries/exits
  kernel launches in blocks of a whole wavefront there, and the refusals and `ziplime sweep doctor`
  name ROCm. Checked on an AMD Instinct MI300X. See "A sweep on an AMD GPU" in
  docs/backtesting/sweep-performance.md.
- **GPUs and architectures.** docs/backtesting/gpu-architectures.md: which backend runs where, and
  the same `handle_data` sweeps measured on an Apple M3, an RTX 5090, an RTX 3090 and an MI300X —
  the same best combination on all of them, and how fast each was.
- **Sweeps on synthetic data.** `"synthetic": {"paths": 100, "families": [...]}` in a sweep file
  (or `ziplime sweep run --paths 100 --family stationary:20 --family meboot`) also runs every
  combination on synthetic price paths made from the real bars. The families are block bootstraps
  (`block`, `circular`, `stationary`), the maximum entropy bootstrap (`meboot`, as R's meboot),
  `iid`, `shuffle`, `gbm`, `student_t` and `garch` (filtered historical simulation). The report
  compares the combination chosen on the paths with the real bars, and the real winner with every
  family; it writes `synthetic_summary.csv`, `synthetic_paths.parquet` and
  `best_synthetic_config.json`. See docs/backtesting/synthetic-data.md.
- **`ziplime sweep run | check | init | doctor`.** `run` takes a sweep file and command-line
  overrides (`--backend`, `--grid fast=5,10,20`, `--set field=value`, `--no-compile`, `-o`...);
  `check` runs the grid's corners in seconds and says how the strategy converts; `init` writes a
  sweep file around a strategy's defaults; `doctor` lists the backends this machine has.
- **`ziplime.sweep.sweep_simulation(...)`** takes run_simulation's arguments and a grid;
  `run_sweep(SweepParams(...))` a sweep file's fields.
- **`ziplime run --config-file`** runs a strategy with a config file -- `best_config.json` from a
  sweep, for one.
- **What a sweep converts.** `handle_data`, `before_trading_start` and scheduled functions run for
  every combination at once, including: `data.history` over several instruments (`filter`,
  `group_by("sid").agg(...)`); history read as an array with NumPy, TA-Lib or SciPy on it, and
  models fitted in `initialize` (`context.model.predict_proba(...)`) -- computed on the CPU before
  the run, TA-Lib's window-local functions once over the whole history; lists of instruments
  sorted and cut per combination (`cands.sort(key=...)`, `cands[:2]`); `portfolio.positions`
  looked up by asset; `context.symbol` in `handle_data`; functions defined inside `handle_data`.
  What cannot be converted is named with its line before the first bar, and the sweep runs every
  combination through run_simulation instead.
- **The sweep explains itself.** Next to the results: how each part of the strategy was computed,
  whether the bars were compiled and why not, and warnings for a computation that is NaN at every
  bar and for a parameter that changes nothing.
- **`context.indicators(...)` in `initialize`.** Indicators declared as polars expressions, computed
  once before the first bar and read as `context.signals` -- an alternative to `compute_signals`.

#### Backtest results that change

- **`portfolio.positions.get(asset)`, `positions[asset]`, `asset in positions`.** The map is keyed
  by (exchange, account, asset), and a lookup by the asset alone missed every time: `get`
  returned its default and `in` was False whatever was held, so a strategy that closed what it held
  by checking its position never closed anything. An asset now finds its position; the tuple keys
  and iteration are unchanged.
- **`order_target_percent` / `order_percent` with `FixedBasisPointsSlippage`.** The volume cap
  subtracted what the *last instrument simulated* had filled in the bar rather than this
  instrument's fills, so a busy bar in one stock could cut an order in another to zero. It now
  subtracts this instrument's own fills.
- **`EODCancel`.** Cancelling at the session end raised "dictionary changed size during iteration"
  at the first session with an open order, which skipped that session's end-of-day bookkeeping.
  Every open order is now cancelled.

#### Fixed

- **`data.current` / `data.history` in `before_trading_start` on intraday runs** raised: the
  previous market minute came back in UTC and the bundle's dates are in the calendar's zone. They
  now read as of the previous session's close.
- **`data.history(frequency=1 day)` on an intraday bundle** built each day from the last minute of
  every field -- a daily "open" was the last minute's, "volume" one minute's -- and with several
  instruments cut the result across all of them, so only the last came back. Days are now OHLCV
  (first, max, min, last, sum) and cut per instrument.
- **`StopOrder`** raised TypeError when placed (`get_limit_price` took `_is_buy` while `Order`
  passes `is_buy=`).
- **`FixedSlippage`** raised TypeError on every fill (`process_order` had no `price`); it now fills
  the open amount at the bar's price plus half the spread.
- **Algorithm and config files** are read as UTF-8 whatever the system locale; on a server with an
  ASCII locale a strategy with non-ASCII comments failed to load.

### Version 2.11.0

Each listing now keeps its own venue's trading hours, so one backtest can hold instruments from
markets that keep different hours. Extensions can now add venues, calendars and data-source
addresses without patching ziplime. `zipfinam` uses this for the Moscow Exchange: MISX, the FORTS
calendar and MOEX AlgoPack.

#### Upgrading from 2.10.3

- **Single-market backtests are unchanged.** A run whose listings all trade on the simulation's
  own calendar gives the same results as on 2.10.3.
- **Moscow venues need zipfinam.** ziplime ships no calendar for MISX or RTSX. Install `zipfinam`
  (1.26.10.1 or later) to have Moscow listings judged by their own hours; without it they are
  judged by the simulation's calendar, as before.

#### Backtest results that change

- **A listing is judged by its own venue's hours.** Before, every listing was judged by the
  simulation's calendar. On an hourly run clocked on Moscow, an American ETF counted as tradeable
  at 10:01 Moscow, which is 03:01 in New York. The order then failed with
  `CannotOrderDelistedAsset: no last price`. Now `data.can_trade` asks the listing's venue. On a
  daily run it asks whether the venue traded that day; on an intraday run it asks about the minute.
- **`context.rebalance` leaves a leg alone while its market is shut.** That leg is neither ordered
  to its target nor closed. Before, the run failed on the first bar where one leg's market was
  closed.

#### New

- `ziplime.utils.venue_calendars`:
  - `calendar_for_mic` gives the calendar a venue keeps, for the US equity venues, CME, NYMEX,
    ICE and the crypto venues;
  - `clock_calendar_name` gives one calendar wide enough for every venue in a run: `24/5` when
    their hours differ, `24/7` when one never closes;
  - `ZIPLIME_VENUE_CALENDARS` overrides the table, as `MIC=CALENDAR,...`.
- **Extension point for venues.** `register_venue_calendars({MIC: calendar})`, or the
  `ziplime.venue_calendars` entry point group, which is loaded on first lookup.
- **Extension point for data-source addresses.** `register_address_scheme(scheme, mount)`, or the
  `ziplime.address_schemes` entry point group. A strategy then reads
  `data.history(..., data_source="scheme://...")` with no registration of its own. `hf://` stays
  built in.
- `ExchangeAsset.trading_calendar`: the listing's own venue calendar, or `None` for a venue not
  in the table. Callers then fall back to the simulation's calendar.
- Ordering a listing whose market is closed now says so and names both calendars. Before, the
  error blamed delisting.

### Version 1.10.16
- Raise proper exception when data bundle is not found

### Version 1.9.18
- Add asset mic in database
- Add default assets database file
- Update examples
- Add cache of currencies query
- Allow calendar dates older than 2005

### Version 1.8.21
- Use scan_parquet to load bundle data partially
- Throw proper error in bundle when data is missing
- Add an option to configure the logging level and log to the file
- Add a start / end auction date to allow getting open/close prices at a specific time in the day when running daily simulation for a minute data bundle
- Allow custom aggregations when using aggregated low-frequency data bundle in simulation
- Symbols universe
- Support for ingesting market data from Yahoo Finance

### Version 1.7.14
- Custom data - Limex fundamental data ingestion
- Add all frequencies supported by polars
- Allow both string and timedelta as frequency
- Add option to use custom CSV data
- Use float for all decimal types

### Version 1.6.26
- Add the forward_fill_missing_ohlcv_data parameter to the BundleService ingest_data function

### Version v1.6.11
- Add an option to continue running algorith on error - stop_on_error

### Version v1.6.2
- Remove uvloop to improve compatibility with Windows OS

### Version v1.5.30
- Add additional ETF/stock symbols
- Log warning if the price is missing when updating the last sale price in positions dict

### Version v1.5.29
- Add ETF symbols to all symbols list

### Version v1.5.28
- Fix simulation error when data is missing during ingestion
