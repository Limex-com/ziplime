# WIP
# Algorithm File Structure in Ziplime

Algorithm file is the specification of your algorithm. It is a python script with functions that `ziplime` will run during backtesting.

## `initialize`

This function is called once at the start of the simulation. 

```python
from ziplime.trading.trading_algorithm import TradingAlgorithm

async def initialize(context: TradingAlgorithm):
    pass
```

- `context`: The algorithm context object that holds your algorithm's state

## `handle_data`

Called for every trading bar according to your emission rate.

- `context`: The algorithm context object
- `data`: Current market data snapshot

## Optional Functions

### before_trading_start(context, data)

Called before the start of each trading day.

- `context`: The algorithm context object
- `data`: Pre-market data snapshot

### analyze(context, results)

Called after the simulation completes to analyze results.

- `context`: The algorithm context object
- `results`: DataFrame containing simulation results



### Example algorithm