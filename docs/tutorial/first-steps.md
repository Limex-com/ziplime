# First Steps

This page takes you from a fresh install to a working daily backtest in four steps.

---

## Step 1 — Install Ziplime

=== "pip"

    ```bash
    pip install ziplime
    ```

=== "Poetry"

    ```bash
    poetry add ziplime
    ```

Ziplime requires **Python 3.12+**.

---

## Step 2 — Ingest historical data

Before running a backtest you need local market data. Ziplime stores data in Parquet bundles inside `~/.ziplime/data/`.

### Option A: Yahoo Finance (free, no API key needed)

```python
# ingest_yahoo.py
import asyncio
import datetime
import pathlib

from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.data.data_sources.yahoo_finance_data_source import YahooFinanceDataSource

async def main():
    symbols    = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    start_date = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    end_date   = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.joinpath("data", "assets.sqlite")),
    )

    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",
        bundle_name="my_daily_data",              # any name you choose
        data_bundle_source=YahooFinanceDataSource(),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
    )
    print("Ingestion complete.")

asyncio.run(main())
```

Run it once and the data is cached locally:

```bash
python ingest_yahoo.py
```

### Option B: LimexHub (professional data)

Set your API key as an environment variable, then call `LimexHubDataSource.from_env()`. See [LimexHub data source](../data-ingestion/data-sources/market-data/limexhub.md) for full details.

---

## Step 3 — Write your algorithm

Create a file `my_algo.py`:

```python
# my_algo.py
from ziplime.finance.execution import MarketOrder

async def initialize(context):
    """Called once at the start of the simulation."""
    context.assets = [
        await context.symbol("AAPL"),
        await context.symbol("MSFT"),
        await context.symbol("GOOGL"),
    ]

async def handle_data(context, data):
    """Called on every bar (daily in this example)."""
    target = 1.0 / len(context.assets)          # equal-weight portfolio
    for asset in context.assets:
        await context.order_target_percent(asset=asset, target=target, style=MarketOrder())
```

See [Algorithm file structure](algorithm-file-structure.md) for the full list of lifecycle functions and what you can do inside each one.

---

## Step 4 — Run the backtest

```python
# run_backtest.py
import asyncio
import datetime
import pathlib
import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.utils.bundle_utils import get_bundle_service

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
        symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META"],
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
    )

    print(result.perf.head(10).to_markdown())

asyncio.run(main())
```

Run it:

```bash
python run_backtest.py
```

You will see a table with daily performance metrics — portfolio value, returns, positions, and more.

---

## What's next?

- Learn how lifecycle functions work in detail → [Algorithm file structure](algorithm-file-structure.md)
- Tune commission, slippage, and position limits → [Algorithm parameters](algorithm-parameters.md)
- Add fundamental data to your strategy → [Data ingestion](../data-ingestion/index.md)
