# Metastrategies

A metastrategy is an algorithm that runs other algorithms and trades them as assets. It declares
the strategies it runs in `initialize`, reads their state on every bar — portfolio, positions,
Sharpe ratio over any window — and buys and sells *units* of them with the ordinary order methods.
Each strategy can also be a whole parameter cluster: every combination of a grid, each run as its
own strategy, side by side.

```python
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.strategy1 = await context.strategy("s1")        # s1.py, next to this file
    context.strategy2 = await context.strategy("s2")


async def handle_data(context, data):
    cash1 = context.strategy1.portfolio.cash
    sharpe1 = context.strategy1.performance.sharpe
    sharpe2 = context.strategy2.performance.sharpe
    if sharpe1 > sharpe2 and cash1 > 900_000:
        await context.order(context.strategy1, 100_000, style=MarketOrder())
```

A metastrategy is an ordinary algorithm file, so `run_simulation` runs it, and `analyze`,
`schedule_function`, `record` and orders for real listings all work in it as usual. Each strategy's
own result — the result it would have had if run alone — is in `result.strategies`.

## Declaring strategies

```python
context.trend = await context.strategy("trend")
context.mr = await context.strategy("strategies/mean_rev.py", config={"lookback": 20})
context.bb = await context.strategy("bollinger",
                                    grid={"bb_period": [10, 20, 30],
                                          "bb_std": {"from": 1.5, "to": 2.5, "step": 0.5}},
                                    where="bb_period >= 20 or bb_std < 2.5")
```

`context.strategy` is called in `initialize` only. Its arguments:

| Argument | |
|---|---|
| `name` | The strategy's algorithm file. A path ending in `.py` is relative to the metastrategy's file; a bare name is looked up as `<name>.py`, then `strategies/<name>.py`, beside it |
| `config`, `config_file` | The strategy's `AlgorithmConfig`: a dict, a JSON file, or both (the dict wins). Unknown fields are an error |
| `grid`, `where` | A parameter cluster, in the [sweep file's syntax](parameter-sweeps.md#the-sweep-file): lists, `{"from", "to", "step"}` ranges, `{"path", "values"}` for nested fields, and a `where` condition. Returns a `StrategyCluster` |
| `capital` | The strategy's own starting cash; the metastrategy's by default. Its NAV is a return, so this only decides how its orders round to whole shares |
| `label` | Its name in results. Default: the file's name, and for cluster members `bollinger[bb_period=20, bb_std=2.0]` |
| `commission` | A commission model charged when the metastrategy trades units of this strategy, e.g. `PerDollar(0.0005)`. Default: free |

Each strategy runs with the same data, calendar, clock, cost models, execution timing and leverage
limit as the metastrategy, and a book of its own: cash, positions, orders, ledger, metrics.

## Reading a strategy

| `handle.` | |
|---|---|
| `portfolio`, `account`, `positions` | The strategy's own book, as of this bar |
| `performance` | Its statistics as of this bar — see below |
| `nav` | The price of one unit: `1.0` at the start, `1 + returns` since |
| `units`, `value`, `weight` | What the metastrategy holds of it: units, `units × nav`, and that as a share of the metastrategy's portfolio value |
| `context` | The strategy's own `context` — anything its `initialize` and `handle_data` set: `handle.context.signal` |
| `config`, `params` | Its `AlgorithmConfig`; and the grid values it runs with (empty outside a cluster) |
| `recorded_vars` | What it has `record`ed |
| `label`, `name`, `asset` | Its unique name, its file's name, and the listing its units trade as |
| `result` | After the run: its `TradingAlgorithmExecutionResult` |

### Performance

`handle.performance` computes the run report's statistics over the strategy's daily returns, today's
partial return included — on the last bar, `performance.sharpe` is the `sharpe` its result
reports. `performance.last(n)` is the same over the last `n` sessions.

| | |
|---|---|
| `returns` | Total return since the start; compounded over the window for `last(n)` |
| `sharpe`, `sortino`, `calmar` | As in the report |
| `volatility`, `annual_return` | Annualised |
| `max_drawdown` | Negative, as in the report: `-0.12` is a 12% drawdown |
| `days` | How many sessions of returns there are |
| `daily_returns` | The returns themselves, as a NumPy array |
| `pnl`, `portfolio_value` | Of the strategy's own book |

A statistic that cannot be computed yet — a Sharpe ratio from one session — is `nan`. Every
comparison with `nan` is `False`, so `if sharpe1 > sharpe2:` simply does not fire until there is
a record to compare; test `performance.days` when the difference matters.

## Trading strategies

A strategy is ordered like an asset. Every order method takes a handle in place of a listing:

```python
await context.order(context.trend, 100_000, style=MarketOrder())                 # units
await context.order_value(context.trend, 250_000, style=MarketOrder())
await context.order_target_percent(context.trend, 0.5, style=MarketOrder())
await context.rebalance({context.trend: 0.6, context.mr: 0.35})                  # everything else closed
await context.rebalance({context.trend: 0.5, sber: 0.4})                         # strategies and listings together
```

`context.order(context.trend.portfolio, ...)` — the strategy's portfolio in place of the handle —
means the same thing.

* **A unit is worth the strategy's NAV.** The NAV starts at 1.0, so on the first bar 100,000 units
  are 100,000 of the strategy.
* **Fills are whole and at the NAV of the bar they execute on.** With `same_bar_execution=False`
  an order placed on a bar fills at the next bar's NAV, as an order for a listing fills at the next
  bar's price; with `True`, at this bar's.
* **No slippage**; the strategy's own fills already paid it. No commission unless the strategy was
  declared with one.
* The position is an ordinary position of the metastrategy, on the `STRATEGIES` venue:
  `context.portfolio`, leverage, `max_leverage` and the performance report all include it.

## Parameter clusters

```python
context.bb = await context.strategy("bollinger", grid={"bb_period": [10, 20, 30],
                                                       "bb_std": [1.5, 2.0, 2.5]})
```

`context.bb` is a `StrategyCluster`: a sequence of nine handles — index it, iterate it, `len` it —
with ranking on top. `by` is a performance metric's name or a function of a handle; `window`
restricts it to the last `n` sessions. Members with no value yet rank last.

| | |
|---|---|
| `best(by="sharpe", window=None)` | The leader |
| `top(n, by, window)` | The `n` best, best first |
| `ranked(by, window)` | All of them, best first (`volatility` ranks smallest first) |
| `find(bb_period=20, bb_std=2.0)` | The member running exactly these values |
| `to_frame(window=None)` | One row per member: its parameters and every metric |

```python
async def reallocate(context, data):            # scheduled monthly
    best = context.bb.top(3, by="sharpe", window=60)
    await context.rebalance({member: 0.95 / 3 for member in best})
```

**A cluster or a sweep?** A [sweep](parameter-sweeps.md) runs thousands to millions of
combinations at once, offline, and reports which was best over the whole period. A cluster runs
tens of combinations through the engine, each with its own book, in step with the metastrategy —
so the metastrategy can choose among them *as the run goes*, on what it knew at each bar: walk-
forward parameter selection, or an ensemble of the best few. Sweep to find the region; cluster
to trade it.

## How it runs

On every clock event — session start, before trading start, bar, session end — every strategy is
stepped first, in the order declared, and the metastrategy second. So when the metastrategy's
`handle_data` runs on a bar, each strategy's `handle_data` has run on that bar, its fills for the
bar are in its book, and its portfolio is marked at the bar's prices. Nothing is a bar ahead.

A strategy runs exactly as it would alone: nothing the metastrategy does changes what it does.
`result.strategies[label]` is identical to `run_simulation` of that file with that config.

An exception in a strategy is recorded in its result's `errors` and the strategy carries on, as a
run does; with `stop_on_error=True` the run stops.

A strategy can itself be a metastrategy.

## What this models, and what it does not

The metastrategy trades the strategies' **equity curves**: its exposure to a strategy is
`units × NAV`, and the strategy trades its own capital regardless. This is how an allocator
between strategies, an on/off switch on a strategy's own record, or a walk-forward choice of
parameters is backtested.

What it does not model: the strategies' positions netted into one account. In a real account,
holding 30% of a strategy means holding 30% of each of its positions, and switching between two
strategies means trading the difference between their books — which may be little, if they hold
similar things. Charge that with `commission=` where it matters.

Not supported yet: live trading (each strategy needs a simulated book), `currency_reporting`,
trading controls (`set_max_position_size` etc.) on strategy units, and fractional units. A
strategy's own `analyze` is not called.
