# Release Notes

### Version 2.11.3

Private user datasets: `dataset://<name>`.

A user's own Parquet file is read the way an `hf://` dataset is, and only by that user:

```python
rows = await data.history(assets=context.universe, fields=["score"], bar_count=20,
                          data_source="dataset://my-signals")
```

#### Added

- **`dataset://<name>`** (`ziplime.data.data_sources.user_datasets`). It reads
  `s3://<bucket>/users/<user_id>/<name>.parquet` under the point-in-time rules of `hf://`, because it
  hands a lazy frame to `HuggingFaceDataSource.from_frame`. Those rules are the knowledge-date index,
  the event-date floor, tickers mapped to sids, and `data.current` by `Resolution`.
  - **Isolation.** The user and the bucket come only from the run's address context, never from the
    strategy. The address carries a name and nothing else: `dataset://<uuid>/x`, `dataset://../x`
    and `dataset://X` are refused. Without a user or a bucket, as in a local run, the scheme refuses
    instead of returning nothing.
  - **Reading.** The object is scanned in place with the streaming engine. Only the role columns and
    the fields the strategy reads are fetched; a later read of another field widens the scan. One
    `HeadObject` per run gives the size, checked against a limit before any data is read
    (`ZIPLIME_USER_DATASET_MAX_BYTES` or the context's `user_dataset_max_bytes`, 1 GiB by default),
    and the ETag.
  - **Cache.** Mounted frames are kept in memory keyed by `(user_id, name, ETag, window, fields,
    resolution)`. A re-upload, which brings a new ETag, is read again and the old entry is dropped.
    The cache is bounded by `ZIPLIME_USER_DATASET_CACHE_MB`, 512 MB by default.
  - **Errors.** `UserDatasetError` carries one plain line. It never contains a traceback, the bucket
    or the user: `403 - dataset does not exist: 'x'`, `invalid dataset address '...'`,
    `dataset:// is only available in hosted backtests`, `dataset 'x' cannot be read: ...`,
    `dataset 'x' is 1.3 GiB, over the 1.0 GiB limit`.
- **`run_simulation(..., address_context={...})`** and the same parameter on `run_simulation_iter`.
  The hosting runtime uses it to give the run what a mount needs to know about it, such as the
  requesting user. It is set for the duration of the run only, and the algorithm copies it when it
  is built. A worker that runs one user's job after another's therefore never hands the second user
  the first user's context. Every scheme mount receives it as keyword arguments, and mounts ignore
  the ones they do not use, so `hf://` and `algopack://` are unchanged.
- **`context.user_dataset(name, fields=None, resolution=None)`** is the explicit form of the address.
  Use it to fix the columns read, or to collapse a restated period with `Resolution.COALESCE`. The
  default is `LATEST_ROW`.

#### Changed

- `HuggingFaceDataSource._collect_kwargs` lets a subclass choose the polars engine its materialise
  step collects with. It is empty for `hf://`, which therefore behaves exactly as before.

### Version 2.11.2

Futures chains per exchange, and option bars by the vendor's security id.

One asset database can now hold the same futures root on several exchanges. `NG` is natural gas
on the Moscow Exchange (100 MMBtu) and on NYMEX (10,000 MMBtu). Before, the second chain was
skipped on import without a word, and its contracts were filed under the first chain's
specification.

#### Upgrading from 2.11.0

- **The asset database gains a migration** (`f3a9d6b2c814`). A chain is keyed by `(root_symbol, mic)`
  and `futures_contracts.root_symbol` is no longer a foreign key. Older ziplime cannot open a
  database migrated this way.
- **A bare root on several exchanges is ambiguous.** `front_contract`, `futures_chain` and
  `continuous_future` raise `AmbiguousRootSymbol` unless the exchange is named: `"NG@XNYM"` or
  `mic="XNYM"`. `ZIPLIME_FUTURES_DEFAULT_MIC` (a comma-separated list) sets the exchange used when
  none is named. A root listed on one exchange works as before.
- **`get_futures_roots()` is keyed by `(root_symbol, mic)`.** It was keyed by root symbol.

#### Added

- **`context.symbol()` finds futures contracts.** `symbol("MMZ6@RTSX")` is the December
  mini-index future and `symbol("IMOEXF@RTSX")` the perpetual one, with no `asset_type`. An equity
  is still looked for first, so a ticker that is both keeps meaning the share. Passing
  `asset_type` restricts the lookup to that kind, as before; its default is now `None`
  (equity, then futures contract) instead of `AssetType.EQUITY`.
- **A continuous future is traded as one instrument, and the engine rolls it.**
  `si = await context.continuous_future("Si")` then `await context.order_target(si, 2, ...)` --
  every order method and `rebalance` accept the specifier and trade the contract its roll rule
  names for the bar. When the front changes, the position is moved at the start of that bar,
  before `handle_data`: the outgoing contract is closed and the same amount opened in the incoming
  one, as two market orders with the usual commission and slippage.
  `portfolio.get_asset_positions_amount(si)` (and `get_asset_positions_value`) report the chain's
  position. A strategy no longer has to remember which contract it holds, compare it with the front
  and roll by hand; `front_contract` still works for one that wants to.
- **`continuous_future(..., roll_before_days=7)`**: each specifier rolls on its own offset -- days
  before the contract's auto close date for the `calendar` rule, the latest roll for `volume` --
  instead of the data bundle's one setting. `None` keeps the bundle's.

#### Changed

- **`continuous_future` rolls by calendar by default** (`roll="calendar"`, seven days before auto
  close). It rolled by volume, one day before auto close. Pass `roll="volume"` for the old rule.
- **Ordering a continuous future no longer raises.** It used to be refused as a data specifier.
- **A continuous future's sid is a 62-bit digest** (from 2**62 up) of the fields it was spelled
  from. The spelled-out digits outgrew int64 for any root longer than a few letters -- splicing
  the history of `IMOEXF@RTSX` died casting the sid -- and the new value never collides with a
  listing's sid. Sids of continuous futures change; nothing stores them.

#### Fixed

- **A reused contract ticker is two contracts.** The Moscow Exchange writes the year as one digit,
  so `SiH4` is both the March 2014 and the March 2024 delivery. A contract's identity now includes
  its expiry. Before, the second contract was dropped on import, and its listing pointed at the
  first contract.
- **A continuous future's sid includes its exchange.** `NG@RTSX` and `NG@XNYM` got the same sid,
  and so did `Si` (Moscow) and `SI` (COMEX).
- **CBOT and CBOE Futures listings use their own calendars.** XCBT now maps to CMES and XCBF to
  XCBF. Before, they used the simulation's calendar.
- **gRPC option chains fetch bars by security id.** `GrpcOptionChainSource.bars` hands the ids
  `GetOptions` returned to a source that accepts them (`security_ids`, in the private gRPC wheel
  0.3.0). The candle service mostly does not answer an option by its OCC ticker.

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
