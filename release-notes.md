# Release Notes

### Version 2.10.3

**Upgrading from 1.19 means rebuilding the asset database.** 2.10 restarted the schema history, so
a database created by 1.19 cannot be migrated in place; every command now says so and stops. Run
`ziplime ingest-assets --clear`, then re-ingest your bundles: they are keyed by sids from the old
database.

**Backtest results change.** These are fixes, not regressions, but a stored result will not
reproduce:

- Next-bar execution showed `handle_data` the portfolio from before the bar's fills, so every
  `order_target*` call re-sent a difference already traded. A long-only daily rebalance could go
  short, reach 4.5x leverage and run cash negative. Next-bar is the default for the CLI and the
  MCP server, so any rebalancing strategy run through them moves.
- The futures, commission, cost-basis, expiry and open-order fixes listed in
  `changes-overview.md`, section 1. Several affect equity-only runs.

**Installing.** `pip install ziplime` no longer pulls pytest, coverage, vectorbt or setuptools.
Optional features are extras:

- `ziplime[mcp]` — the local MCP server (`ziplime mcp`)
- `ziplime[huggingface]` — `hf://` point-in-time datasets
- `ziplime[options]` — option pricing and Greeks
- `ziplime[numba]` — the compiled vector kernel
- `ziplime[all]`

**New**
- Futures, bonds and options as instruments, in one portfolio with equities
- `ziplime` command line: `ingest-assets`, `ingest`, `run`, `bundles`, `clean`, `providers`, `mcp`
- Local MCP server over stdio, 14 tools to ingest, write, check and backtest strategies
- Point-in-time datasets mounted from the Hugging Face Hub
- A native vectorised simulation kernel, and vectorised signals inside an event-driven run
- Data connector registry, real futures contract chains on Yahoo Finance
- Emission rates between one minute and one day; history by calendar time
- Live trading on Lime: a single-tick clock, live venues, capital allocation
- Bundles are stored as Delta Lake tables

**Fixed**
- First run on a machine with no `~/.ziplime` failed with "unable to open database file"
- `ingest-assets` from Yahoo stored listings on unmapped venues (NGM, SHZ, ...) with no exchange,
  and a `hf://` mount then failed on every bar while the run exited 0
- `ziplime mcp` without the `mcp` package printed a traceback instead of the install hint
- Benchmark returns are aligned to sessions, not to benchmark bars
- `schedule_function` and several zipline features that accepted arguments and did nothing

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
