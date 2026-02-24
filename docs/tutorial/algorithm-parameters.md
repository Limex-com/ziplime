# Algorithm Parameters

This page describes every parameter available when calling `run_simulation()` (Python API) or `ziplime run` (CLI).

---

## `run_simulation()` reference

```python
from ziplime.core.run_simulation import run_simulation

result = await run_simulation(
    start_date=...,
    end_date=...,
    trading_calendar=...,
    algorithm_file=...,
    total_cash=...,
    market_data_source=...,
    emission_rate=...,
    # --- optional ---
    custom_data_sources=...,
    config_file=...,
    benchmark_asset_symbol=...,
    benchmark_returns=...,
    stop_on_error=...,
    asset_service=...,
    equity_commission=...,
    futures_commission=...,
    equity_slippage=...,
    futures_slippage=...,
    clock=...,
)
```

### Required parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `start_date` | `datetime` | First day of the simulation (timezone-aware) |
| `end_date` | `datetime` | Last day of the simulation (timezone-aware) |
| `trading_calendar` | `str` | Exchange calendar identifier, e.g. `"NYSE"` |
| `algorithm_file` | `str` | Absolute path to your algorithm `.py` file |
| `total_cash` | `float` | Starting capital in the portfolio's base currency |
| `market_data_source` | bundle | Loaded data bundle returned by `bundle_service.load_bundle()` |
| `emission_rate` | `timedelta` | How often `handle_data` is called — `timedelta(days=1)` for daily, `timedelta(minutes=1)` for minute-level |

### Optional parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `custom_data_sources` | `list` | `[]` | Additional (non-OHLCV) data bundles, e.g. fundamental data |
| `config_file` | `str` | `None` | Path to a JSON file loaded into `context.algorithm.config` |
| `benchmark_asset_symbol` | `str` | `None` | Ticker to use as benchmark for alpha/beta calculations |
| `benchmark_returns` | Polars Series | `None` | Provide pre-computed benchmark returns instead of a symbol |
| `stop_on_error` | `bool` | `False` | If `True`, raise exceptions; if `False`, log and continue |
| `asset_service` | `AssetService` | auto | Asset database service; defaults to `~/.ziplime/assets.sqlite` |
| `equity_commission` | `CommissionModel` | `PerShare` | Commission model for equity orders |
| `futures_commission` | `CommissionModel` | `PerContract` | Commission model for futures orders |
| `equity_slippage` | `SlippageModel` | `FixedBasisPointsSlippage` | Slippage model for equity orders |
| `futures_slippage` | `SlippageModel` | `VolumeShareSlippage` | Slippage model for futures orders |
| `clock` | `TradingClock` | `SimulationClock` | Custom clock implementation (e.g. `SingleExecutionClock`) |

---

## Commission models

Commission is the brokerage fee charged per order. Import from `ziplime.finance.commission`.

### `PerShare` (default for equities)

```python
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE

equity_commission = PerShare(
    cost=DEFAULT_PER_SHARE_COST,           # $0.001 per share
    min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,  # $0.0
)
```

| Argument | Default | Description |
|----------|---------|-------------|
| `cost` | `0.001` | Dollar cost per share |
| `min_trade_cost` | `0.0` | Minimum fee per trade |

### `PerContract` (default for futures)

```python
from ziplime.finance.commission import PerContract, DEFAULT_PER_CONTRACT_COST

futures_commission = PerContract(
    cost=DEFAULT_PER_CONTRACT_COST,   # $0.85 per contract
    exchange_fee=0.0,
)
```

### `PerDollar`

A percentage-based commission on the trade value.

```python
from ziplime.finance.commission import PerDollar

commission = PerDollar(cost=0.001)   # 0.1 % of trade value
```

### `NoCommission`

Zero-cost commission useful for research and testing.

```python
from ziplime.finance.commission import NoCommission

commission = NoCommission()
```

---

## Slippage models

Slippage models simulate the difference between the theoretical fill price and the actual execution price. Import from `ziplime.finance.slippage`.

### `FixedBasisPointsSlippage` (default for equities)

```python
from ziplime.finance.slippage import FixedBasisPointsSlippage

equity_slippage = FixedBasisPointsSlippage(basis_points=5)   # 0.05 %
```

### `FixedSlippage`

A flat dollar amount added to every fill.

```python
from ziplime.finance.slippage import FixedSlippage

slippage = FixedSlippage(spread=0.05)   # $0.05 per share
```

### `VolumeShareSlippage` (default for futures)

Proportional to the fraction of daily volume your order represents.

```python
from ziplime.finance.slippage import VolumeShareSlippage

slippage = VolumeShareSlippage(
    volume_limit=0.025,     # max 2.5 % of daily volume per bar
    price_impact=0.1,       # price-impact coefficient
)
```

### `NoSlippage`

Zero slippage — useful for ideal-market research.

```python
from ziplime.finance.slippage import NoSlippage

slippage = NoSlippage()
```

---

## Position and leverage controls

Set inside `initialize()` to enforce risk limits.

```python
from ziplime.finance.controls import MaxLeverage, MaxPositionSize, MaxOrderSize, LongOnly

async def initialize(context):
    context.set_max_leverage(1.5)          # portfolio leverage cap

    context.set_long_only()               # no short positions allowed

    context.set_max_position_size(
        asset=None,                        # None = applies to all assets
        max_shares=1000,
        max_notional=50_000,
    )

    context.set_max_order_size(
        asset=None,
        max_shares=500,
        max_notional=25_000,
    )
```

---

## `load_bundle()` parameters

Before passing a bundle to `run_simulation`, load it with `bundle_service.load_bundle()`.

```python
market_data = await bundle_service.load_bundle(
    bundle_name="my_daily_data",
    bundle_version=None,          # None = latest version
    frequency=datetime.timedelta(days=1),
    start_date=start_date,
    end_date=end_date,
    symbols=["AAPL", "MSFT"],
    # --- optional ---
    start_auction_delta=datetime.timedelta(minutes=15),
    end_auction_delta=datetime.timedelta(minutes=15),
    aggregations=[...],           # custom Polars aggregations
)
```

| Parameter | Description |
|-----------|-------------|
| `bundle_name` | Name you gave the bundle during ingestion |
| `bundle_version` | Specific version; `None` picks the latest |
| `frequency` | Target bar frequency. Must be ≥ ingested frequency |
| `start_date` / `end_date` | Date range to load (must be inside the ingested range) |
| `symbols` | List of ticker strings to load |
| `start_auction_delta` | Offset from market open for OHLCV aggregation |
| `end_auction_delta` | Offset from market close for OHLCV aggregation |
| `aggregations` | Custom Polars expressions for OHLCV roll-up (e.g. when loading minute data as daily) |

### Default aggregations when loading higher-frequency data as daily bars

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
```

---

## CLI reference

```bash
ziplime run \
  --algofile my_algo.py \
  --bundle my_daily_data \
  --start-date 2024-01-03 \
  --end-date 2025-01-01 \
  --capital-base 100000 \
  --emission-rate 1d \
  --trading-calendar NYSE \
  --benchmark-symbol AAPL \
  --output results.csv
```

| Flag | Default | Description |
|------|---------|-------------|
| `--algofile` | — | Path to algorithm Python file |
| `--bundle` | — | Bundle name to use for market data |
| `--start-date` | — | Simulation start (`YYYY-MM-DD`) |
| `--end-date` | — | Simulation end (`YYYY-MM-DD`) |
| `--capital-base` | `10000` | Starting capital |
| `--emission-rate` | `1d` | Bar frequency: `1m`, `5m`, `1h`, `1d`, etc. |
| `--trading-calendar` | `NYSE` | Exchange calendar |
| `--benchmark-symbol` | — | Ticker for benchmark comparison |
| `--benchmark-file` | — | CSV file with benchmark returns |
| `--no-benchmark` | `False` | Disable benchmark entirely |
| `--output` | `-` (stdout) | Output file for performance results |
| `--exchange-type` | `simulation` | `simulation` or `lime-trader-sdk` |
| `--bundle-storage-path` | `~/.ziplime/data` | Path to local bundle storage |

---

## Streaming results with `run_simulation_iter`

Use `run_simulation_iter` to process results incrementally as the simulation runs:

```python
from ziplime.core.run_simulation import run_simulation_iter

async for status in run_simulation_iter(
    start_date=start_date,
    end_date=end_date,
    # ... same parameters as run_simulation ...
):
    if status.errors:
        print("Error:", status.errors)
    if status.result:
        # Final result available
        print(status.result.perf.head(10))
    else:
        # Intermediate update
        print(f"Daily perf: {status.daily_perf}")
```
