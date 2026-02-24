# Algorithm File Structure

A Ziplime algorithm is a plain Python file that defines one or more lifecycle functions. Ziplime calls these functions at specific points in the simulation. You do **not** need to subclass anything — just define the functions below.

---

## Lifecycle functions

### `initialize(context)` — required

Called **once** at the very start of the simulation. Use it to:

- Look up assets with `context.symbol()`
- Store state on `context` (it persists across all bars)
- Register scheduled functions
- Attach pipelines

```python
async def initialize(context):
    context.spy = await context.symbol("SPY")
    context.traded_today = False
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `TradingAlgorithm` | Mutable algorithm state object |

---

### `handle_data(context, data)` — required

Called **on every bar** according to the `emission_rate` you set when running the simulation (e.g. daily, every minute). This is where your trading logic lives.

```python
async def handle_data(context, data):
    price = data.current(context.spy, "close")
    if price > context.entry_price:
        await context.order_target_percent(context.spy, 1.0)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `TradingAlgorithm` | Algorithm state (same object as in `initialize`) |
| `data` | `BarData` | Current market snapshot — prices, volumes, custom data |

---

### `before_trading_start(context, data)` — optional

Called **once per trading day**, shortly before the market opens. Use it to prepare daily state, fetch pipeline results, or filter your universe.

```python
async def before_trading_start(context, data):
    context.todays_universe = await context.pipeline_output("my_pipeline")
```

---

### `analyze(context, results)` — optional

Called **once after the simulation ends**. Use it to print reports, plot equity curves, or persist results.

```python
async def analyze(context, results):
    print(results.perf.select(["date", "portfolio_value"]))
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `context` | `TradingAlgorithm` | Final algorithm state |
| `results` | `TradingAlgorithmExecutionResult` | Full backtest results |

---

## The `context` object

`context` is an instance of `TradingAlgorithm`. It gives you access to the entire Ziplime API.

### Asset lookup

```python
# Look up by ticker symbol
asset = await context.symbol("AAPL")

# Look up with explicit exchange (MIC code)
asset = await context.symbol("AAPL@XNYS")

# Look up multiple symbols at once
assets = await context.symbols("AAPL", "MSFT", "GOOGL")
```

### Placing orders

```python
from ziplime.finance.execution import MarketOrder, LimitOrder

# Buy/sell a fixed number of shares
await context.order(asset, 100)            # buy 100 shares
await context.order(asset, -50)            # sell 50 shares

# Target a specific number of shares
await context.order_target(asset, 200)

# Target a dollar value
await context.order_target_value(asset, 10_000)

# Target a portfolio percentage  (most common in practice)
await context.order_target_percent(asset, 0.25)   # 25 % of portfolio

# Specify order style
await context.order(asset, 100, style=MarketOrder())
await context.order(asset, 100, style=LimitOrder(limit_price=150.0))
```

### Reading current prices

```python
# Inside handle_data, use the data argument:
close  = data.current(asset, "close")
volume = data.current(asset, "volume")
ohlcv  = data.current(asset, ["open", "high", "low", "close", "volume"])

# Historical window (last N bars)
history = data.history(asset, "close", bar_count=20, frequency="1d")
```

### Portfolio and positions

```python
portfolio  = context.portfolio
cash       = portfolio.cash
pnl        = portfolio.pnl
positions  = portfolio.positions         # dict[Asset → Position]
position   = portfolio.positions[asset]
shares_held = position.amount
```

### Recording custom metrics

```python
context.record(my_signal=signal_value, cash=context.portfolio.cash)
```

Recorded values appear as columns in `result.perf`.

### Scheduling functions

```python
from ziplime.utils.events import date_rules, time_rules

async def initialize(context):
    context.schedule_function(
        rebalance,
        date_rule=date_rules.week_start(),
        time_rule=time_rules.market_open(minutes=30),
    )

async def rebalance(context, data):
    ...
```

---

## Algorithm configuration (optional)

You can attach a typed JSON config to your algorithm. Create a class that extends `BaseAlgorithmConfig`:

```python
# my_algo.py
from pydantic import BaseModel
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig

class AlgorithmConfig(BaseAlgorithmConfig):
    max_position_size: float = 0.10
    symbols_to_trade: list[str] = []

async def initialize(context):
    cfg = context.algorithm.config      # typed AlgorithmConfig instance
    context.max_pos = cfg.max_position_size
    context.assets = [await context.symbol(s) for s in cfg.symbols_to_trade]
```

Pair it with a JSON file (passed as `config_file` to `run_simulation`):

```json
{
  "max_position_size": 0.15,
  "symbols_to_trade": ["AAPL", "MSFT", "GOOGL"]
}
```

---

## Minimal complete example

```python
# equal_weight.py
from ziplime.finance.execution import MarketOrder

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN"]

async def initialize(context):
    context.assets = [await context.symbol(s) for s in SYMBOLS]

async def handle_data(context, data):
    target = 1.0 / len(context.assets)
    for asset in context.assets:
        await context.order_target_percent(asset=asset, target=target, style=MarketOrder())

async def analyze(context, results):
    final_value = results.perf["portfolio_value"][-1]
    print(f"Final portfolio value: ${final_value:,.2f}")
```
