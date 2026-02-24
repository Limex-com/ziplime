# Backtesting

Ziplime's backtesting engine runs your trading algorithm against historical data in an event-driven simulation. Each bar, the engine calls your `handle_data` function with the current market snapshot, executes queued orders with realistic slippage and commission, and tracks portfolio metrics.

---

## Concepts

### Event-driven simulation

The simulation clock ticks forward one bar at a time. For every bar the engine:

1. Updates prices in the `BarData` snapshot
2. Calls `before_trading_start` (once per session, at market open)
3. Calls `handle_data` with the current bar
4. Fills pending orders using the configured slippage model
5. Applies commission to every fill
6. Updates the portfolio ledger and records metrics

### Emission rate

The `emission_rate` parameter controls bar granularity:

| Emission rate | `handle_data` calls per trading day |
|---------------|--------------------------------------|
| `timedelta(days=1)` | 1 (end of day) |
| `timedelta(hours=1)` | ~6–7 |
| `timedelta(minutes=1)` | ~390 |

The emission rate must be less than or equal to the frequency of the ingested data bundle.

### Bundles and frequency conversion

Data bundles store raw bars at a fixed frequency (e.g. 1-minute). When running a daily simulation against a 1-minute bundle, pass custom Polars aggregation expressions so Ziplime knows how to roll up the bars:

```python
import polars as pl

aggregations = [
    pl.col("open").first(),
    pl.col("high").max(),
    pl.col("low").min(),
    pl.col("close").last(),
    pl.col("volume").sum(),
    pl.col("symbol").last(),
]

market_data = await bundle_service.load_bundle(
    bundle_name="my_minute_data",
    frequency=datetime.timedelta(days=1),   # target frequency
    aggregations=aggregations,
    start_auction_delta=datetime.timedelta(minutes=15),
    end_auction_delta=datetime.timedelta(minutes=15),
    ...
)
```

---

## Running a daily backtest

```python
import asyncio
import datetime
import pathlib
import pytz
import polars as pl

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE

async def main():
    start_date = datetime.datetime(2024, 1, 3, tzinfo=pytz.timezone("America/New_York"))
    end_date   = datetime.datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))

    bundle_service = get_bundle_service()
    asset_service  = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.joinpath("data", "assets.sqlite")),
    )

    market_data = await bundle_service.load_bundle(
        bundle_name="my_daily_data",
        bundle_version=None,
        frequency=datetime.timedelta(days=1),
        start_date=start_date,
        end_date=end_date,
        symbols=["AAPL", "MSFT", "GOOGL"],
    )

    result = await run_simulation(
        start_date=start_date,
        end_date=end_date,
        trading_calendar="NYSE",
        algorithm_file="my_algo.py",
        total_cash=100_000.0,
        market_data_source=market_data,
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="AAPL",
        stop_on_error=True,
        asset_service=asset_service,
        equity_commission=PerShare(
            cost=DEFAULT_PER_SHARE_COST,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,
        ),
    )

    print(result.perf.head(10).to_markdown())

asyncio.run(main())
```

---

## Running a minute-level backtest

```python
market_data = await bundle_service.load_bundle(
    bundle_name="my_minute_data",
    frequency=datetime.timedelta(minutes=1),
    start_date=start_date,
    end_date=end_date,
    symbols=["AAPL", "MSFT"],
)

result = await run_simulation(
    ...
    market_data_source=market_data,
    emission_rate=datetime.timedelta(minutes=1),
    ...
)
```

---

## Backtest results

`run_simulation` returns a `TradingAlgorithmExecutionResult` with the following attributes:

| Attribute | Type | Description |
|-----------|------|-------------|
| `perf` | `polars.DataFrame` | Daily performance table (see columns below) |
| `errors` | `list` | Errors collected during the run (when `stop_on_error=False`) |
| `trading_algorithm` | `TradingAlgorithm` | Algorithm instance after the run |

### Performance DataFrame columns

| Column | Description |
|--------|-------------|
| `date` | Bar date |
| `portfolio_value` | Total portfolio value (cash + positions) |
| `returns` | Daily return |
| `pnl` | Daily profit & loss |
| `cash` | Remaining cash |
| `positions` | Open positions snapshot |
| `orders` | Orders placed this bar |
| `transactions` | Filled orders this bar |
| `gross_leverage` | Gross leverage |
| `net_leverage` | Net leverage |
| `alpha` | Rolling alpha vs benchmark |
| `beta` | Rolling beta vs benchmark |
| `sharpe` | Rolling Sharpe ratio |
| `sortino` | Rolling Sortino ratio |
| `max_drawdown` | Maximum drawdown to date |

### Accessing transactions

```python
for bar_transactions in result.perf["transactions"]:
    for txn in bar_transactions:
        print(txn.id, txn.amount, txn.price, txn.realized_pnl)
```

---

## Including fundamental data

Pass additional bundles via `custom_data_sources`:

```python
fundamental_bundle = await bundle_service.load_bundle(
    bundle_name="limex_us_fundamental_data",
    bundle_version=None,
)

result = await run_simulation(
    ...
    custom_data_sources=[fundamental_bundle],
    ...
)
```

Inside your algorithm, access custom data through `data.current(asset, "field_name")` just like price data.

---

## Error handling

Set `stop_on_error=True` to raise the first exception and halt the simulation. Set it to `False` to collect errors and continue running — useful when testing across a large universe where a few missing data points are acceptable.

```python
result = await run_simulation(..., stop_on_error=False)
if result.errors:
    for err in result.errors:
        print(err)
```

---

## What's next?

- All `run_simulation` parameters → [Algorithm parameters](../tutorial/algorithm-parameters.md)
- Writing algorithm logic → [Algorithm file structure](../tutorial/algorithm-file-structure.md)
- Ingesting data → [Data ingestion](../data-ingestion/index.md)
