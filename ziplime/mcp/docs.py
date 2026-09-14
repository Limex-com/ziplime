"""The contract the local engine enforces, in the form a model reads.

**This is not the hosted platform's contract, and the differences matter.**
ziplime.limex.com runs a fork with a sandbox: no imports, exactly two hooks,
no config class. The open-source engine has none of those restrictions — it
imports your file with ``importlib`` and calls what it finds. A model carrying
the cloud rules into a local install writes worse code than it needs to, and
one carrying the local rules into the cloud writes code the sandbox rejects.
So the rules travel with the server that enforces them.
"""

EXAMPLE_STRATEGY = '''\
import structlog

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)


async def initialize(context: TradingAlgorithm):
    context.assets = [await context.symbol("AAPL"), await context.symbol("MSFT")]
    context.fast, context.slow = 20, 50


async def handle_data(context: TradingAlgorithm, data: BarData):
    for asset in context.assets:
        bars = await data.history(
            assets=[asset], fields=["close"], bar_count=context.slow
        )
        if len(bars) < context.slow:
            continue
        closes = bars["close"]
        fast = closes[-context.fast:].mean()
        slow = closes.mean()
        target = 0.5 if fast > slow else 0.0
        await context.order_target_percent(
            asset=asset, style=MarketOrder(), target=target
        )
'''

STRATEGY_RULES = f"""\
WRITING A ZIPLIME STRATEGY (open-source engine, running locally)

STRUCTURE
- `async def initialize(context)` runs once before the first bar.
- `async def handle_data(context, data)` runs on every bar.
- Both are optional as far as the loader is concerned — it substitutes a
  no-op — but a strategy without `handle_data` never trades, which is almost
  always a mistake rather than a design.
- `before_trading_start(context, data)` and `analyze(context, perf)` are
  called if you define them. Define them `async`.
- The file is imported, so module-level code runs. Keep it to definitions.

IMPORTS ARE ALLOWED, AND EXPECTED
- This is a plain Python module: `import numpy as np`, `import polars as pl`,
  `import talib`, anything installed. The hosted platform forbids imports and
  injects those names instead; that restriction is the platform's, not the
  engine's. Locally, import what you use.
- The engine's own types are worth importing for the editor's sake:
  `from ziplime.finance.execution import MarketOrder, LimitOrder`,
  `from ziplime.domain.bar_data import BarData`,
  `from ziplime.trading.trading_algorithm import TradingAlgorithm`.

EVERYTHING THAT TOUCHES THE ENGINE IS AWAITED
- `asset = await context.symbol("AAPL")`
- `bars = await data.history(assets=[asset], fields=["close"], bar_count=30)`
- `await context.order_target_percent(asset=asset, style=MarketOrder(), target=0.5)`
- A missing `await` does not raise. It quietly does nothing and the strategy
  places no orders, which reads as a strategy that found no signal.

SYMBOLS
- `await context.symbol("AAPL")` — a plain ticker is the normal form, and it
  resolves against whichever asset database you ingested.
- `ticker@MIC` only when you actually know the MIC. Inventing one is the usual
  cause of "no asset found".
- A symbol has to be in the bundle the backtest runs against. Ingest first.

DATA IS POLARS, NOT PANDAS
- `bars["close"][-1]`, `bars["close"].to_numpy()`, `bars["close"].mean()`.
- `.iloc`, `.loc`, `.at` and `.iat` do not exist on a Polars frame.

PARAMETERS
- Subclass the config model at module level and pass a JSON file to the run:

      from ziplime.config.base_algorithm_config import BaseAlgorithmConfig

      class AlgorithmConfig(BaseAlgorithmConfig):
          fast: int = 20
          slow: int = 50

  Read it as `context.algorithm.config.fast`.

WHAT BELONGS TO THE RUN, NOT THE CODE
- Dates, capital, bundle, benchmark, calendar, commission and slippage are
  arguments to `run_backtest`. Setting them in code either does nothing or
  contradicts the run.

A COMPLETE EXAMPLE

{EXAMPLE_STRATEGY}
"""

LOCAL_VS_CLOUD = """\
The same engine, two contracts. Locally you own the process, so the engine
imports your file and runs it as written. On ziplime.limex.com the same code
runs inside a sandbox that forbids imports, injects the libraries, and offers
exactly two hooks — because it executes strangers' code on shared machines.

What changes between them: imports (allowed here, refused there),
`before_trading_start` and `analyze` (called here, absent there), the config
class (here, not there), and where data comes from (bundles you ingest here, a
managed catalogue there).

What does not change: two async hooks as the shape of a strategy, awaiting
everything that touches the engine, Polars frames, and plain tickers.
"""
