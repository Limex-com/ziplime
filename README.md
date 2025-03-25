# ziplime

Ziplime is based on zipline and enables using Lime data for backtesting.

Zipline is a Pythonic event-driven system for backtesting, developed and used as the backtesting and live-trading engine
by crowd-sourced investment fund Quantopian. 


## Installation

```Bash
poetry add ziplime
```

## Usage

You can find usage examples below. All commands supported by `zipline` are also supported by `ziplime` with extended
list of parameters for easier usage.

### Data ingestion

Data ingestion works by first fetching historical data and then and then running live data fetch in the background.

Difference from original zipline:

- `--start-date` and `--end-date` parameters - used to fetch bundle data only for specific date period
- `--symbols` parameter - specifies symbols to fetch data for directly in the command
- Running live data fetch in the background

Example:

Ingest data:

```Bash
poetry run python -m ziplime ingest -b lime --frequency 1m --start-date 2024-06-01 --end-date 2024-07-31 --symbols AAPL,TSLA,AMZN
```

Run strategy

```Bash
poetry run python -m ziplime -b lime --start-date 2024-11-06 --end-date 2024-11-27 --emission-rate 1m --capital-base 100000 --benchmark-symbol AAPL -f test.py --trading-calendar NYSE --print-algo
```

Run live trade (still in development)

```Bash
run -b lime --start 2024-12-01 --end 2024-12-31 --data-frequency daily --capital-base 100000 --no-benchmark --broker lime-trader-sdk -f test_live_trade.py --print-algo
```

