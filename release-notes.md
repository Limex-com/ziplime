# Release Notes

### Version 2.10.3

The first release since 1.19.16. Ziplime now trades futures, bonds and options alongside equities in
one portfolio. It has a command line and a local MCP server, mounts point-in-time datasets straight
from the Hugging Face Hub, and runs a strategy live on Lime from the same file it backtests. Many
engine bugs are fixed along the way, so **backtest results change**. Read *Upgrading* first.

#### Upgrading from 1.19.16

- **Rebuild the asset database.** This release restarts the database schema history, so a database
  created by 1.19 cannot be upgraded in place. Every command stops with a message saying so. Run
  `ziplime ingest-assets --clear`, then re-ingest your bundles: they are keyed by sids from the old
  database.
- **Bundles are stored as Delta Lake tables** instead of plain Parquet.
- **Optional features are pip extras.** `pip install ziplime` installs only the engine. Add what
  you use:
  - `ziplime[mcp]`: the local MCP server
  - `ziplime[huggingface]`: `hf://` datasets
  - `ziplime[options]`: option pricing and Greeks
  - `ziplime[numba]`: the compiled vector kernel
  - `ziplime[all]`: everything above

  pytest, coverage, vectorbt and setuptools are no longer installed with ziplime.
- **Breaking API changes:**

  | What | Before | Now |
  | --- | --- | --- |
  | `AssetService.get_exchange_assets_by_symbols` | one `AssetType` | also accepts a sequence, and raises `AmbiguousSymbol` when a ticker resolves under more than one |
  | Ordering a `ContinuousFuture` | undefined behaviour | raises; order the contract `data.current_contract(cf)` returns |
  | `MarketImpactBase.get_txn_volume` | `(data, order)` | `(volume, order)` |
  | `DEFAULT_MOEX_EXCHANGE_FEE` | | renamed `DEFAULT_FUTURE_EXCHANGE_FEE` |
  | `ziplime.data.data_sources.yahoo_finance_*` | | moved under `yahoo/`; the old paths still work and emit `DeprecationWarning` |
  | `DataSource` / `DataBundle` | naive or date bounds accepted | require timezone-aware datetime bounds and an `ExchangeCalendar` |
  | `get_spot_value` | synchronous | `async` |

#### Backtest results that change

Each of these is a fix, but a result stored from 1.19 will not reproduce:

- **Next-bar execution.** `handle_data` used to see the portfolio from before that bar's fills.
  Every `order_target*` call then re-sent an order that had already filled. A long-only daily
  rebalance could go short, reach 4.5x leverage and run cash negative. Next-bar is the default in
  the CLI and the MCP server.
- **Commission now reaches the cost basis**, for equities too. Cash was always right; reported cost
  basis and realised P&L were not.
- **Auto-close works.** A listing past its auto-close date used to stay on the books at a stale
  mark.
- **`get_open_orders(asset)` reports working orders.** It used to return `[]`, so a strategy that
  topped up an unfilled order bought a multiple of its target.
- **Cancelled and rejected orders leave the open-order book.**
- **Trading controls are enforced:** `set_max_position_size`, `set_max_order_size`,
  `set_max_order_count`, `set_long_only` and `set_asset_restrictions`. Before, they accepted limits
  and enforced none.
- **The first session is kept** on exchanges east of UTC; bundle loading used to drop it.
- **`perf["positions"]` holds snapshots**, so recorded rows no longer change as the run proceeds.
- **Benchmark returns are aligned to sessions.** A benchmark missing a day shifted every later
  reading onto the wrong session.
- **The `equity_slippage` and `future_slippage` arguments of `run_simulation` are honoured.** They
  were ignored.
- **Futures variation margin applies in both directions.** A long futures position could not lose
  money before.

`changes-overview.md` gives the effect of each of these on the examples.

#### New

**Asset classes**
- **Futures.** Contract chains, continuous futures with back-adjustment, calendar and volume roll
  finders, margin models, and cash or physical settlement. On the strategy side:
  `context.futures_chain(root)` and `context.front_contract(root)`.
- **Bonds.** Clean and dirty prices, accrued interest, coupons, amortisation, and redemption at par.
- **Options.** Contract size, settlement at intrinsic value, upfront or margined premium,
  Black-Scholes prices and Greeks (`ziplime[options]`), chains and multi-leg structures.
- **One portfolio for all of them.** Equities, bonds, futures and options share one cash balance,
  one ledger and one bundle.

**Data**
- **Data connector registry.** Connectors resolve by name, and third parties can register one
  through the `ziplime.data_providers` entry point. `ziplime providers` lists what an install can
  use.
- **Yahoo Finance.** Instrument catalogue, daily and intraday bars, and real dated futures chains
  (ES, CL, NG, ZC, ...) with exchange contract specifications.
- **Hugging Face Hub.** Mount point-in-time datasets with `data.history(...,
  data_source="hf://owner/name/config")` or `context.huggingface_dataset(...)`; no ingest step is
  needed. Rows are indexed on the date they became public, revisions are pinned for repeatable
  runs, and only the files the window needs are downloaded.
- **Splits and dividends** are stored in the asset database and applied in simulation.

**Running strategies**
- **Command line:** `ziplime providers | ingest-assets | ingest | bundles | clean | run | mcp`.
  Errors go to stderr with a hint on what to do, and the exit code reflects the result.
- **Local MCP server** (`ziplime mcp`, `ziplime[mcp]`). It gives Claude, Cursor or any MCP client
  14 tools to ingest data, write and check strategies, run backtests and compare them. It places no
  orders. It does run the code the agent writes; see `SECURITY.md`.
- **Any emission rate between one minute and one day** (5 minutes, 15, an hour), with
  `intraday_metrics` to emit performance packets on every bar.
- **Vectorised execution.**
  - A strategy can define `compute_signals(context, prices)`: indicators are computed once over the
    whole history, then traded bar by bar through the ordinary blotter.
  - A native vectorised kernel runs parameter sweeps, then replays the winner through the ziplime
    ledger.
- **Live trading on Lime.** A rewritten Lime Trader SDK exchange:
  - async throughout;
  - broker-validated orders rounded to whole lots;
  - fills deduplicated from the account's trades;
  - capital allocation per strategy;
  - a single-tick clock for scheduled deployments;
  - opt-in order guards read from the environment: an order deadline, quantity and notional
    limits, a no-sell switch, a symbol allowlist.

**Strategy API**
- `schedule_function` with `date_rules` and `time_rules` works (month rules could not be built
  before).
- `data.history(..., since=timedelta(...))` reads by calendar time instead of row count.
- `data.returns(assets, bar_count)`, `context.rebalance(weights)`,
  `context.universe_symbols(name)`, `context.is_month_start()` and `context.is_week_start()`.
- `BarData.can_trade` works on listings; it used to raise.

#### Fixed

These are in addition to the changes listed under *Backtest results that change*.
- First run on a machine without `~/.ziplime` failed with "unable to open database file".
- `ingest-assets` from Yahoo stored listings on venues outside its exchange map with no exchange.
  A `hf://` mount then failed on every bar while the run exited 0.
- `ziplime clean` reported success and deleted nothing.
- `NoSlippage` raised on every order. `order_value` with `limit_price` or `stop_price` raised
  `TypeError`.
- Running without a benchmark raised.
- `PerTrade` commission raised `TypeError`; `get_asset_positions_amount` raised `AttributeError`.
- A date-only knowledge column was visible hours early west of UTC.
- `ziplime mcp` without the `mcp` package printed a traceback instead of the install hint.

#### Project

- `SECURITY.md`: how to report a vulnerability, and what running a strategy (or letting an agent
  write one) means.
- The test suite runs in CI on every pull request: 1038 tests.

### Version 1.11.11
- Upgrade limexhub version to 1.10.18
- Add some documentation using mkdocs
- Add GRPC data source
- Allow passing specific equity commission model to run_algorithm function
- Allow ingesting data from multiple exchanges for one asset
- Add ingestion of exchanges
- Allow filtering assets by mic directly in symbol name
- Don't throw error if config file for algorithm is not set

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
