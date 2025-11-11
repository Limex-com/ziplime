## Define symbols, start/end date and data frequency
```python
symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]
start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
end_date = datetime.datetime(year=2025, month=2, day=27, tzinfo=datetime.timezone.utc)
data_frequency = datetime.timedelta(minutes=1)
```

## Initialize client

### Initialize market data source instance
`YahooFinanceDataSource` instance can be created by providing next arguments

```python
market_data_bundle_source = YahooFinanceDataSource(maximum_threads=NUMBER_OF_THREDS_YOU_WANT_TO_USE)
```

Number of threads is optional, it's used to speed up fetching of asset data by executing multiple
API requests at once. If not specified, the maximum number will be set to the number of CPU cores.

## Configure asset service

```python
asset_service = get_asset_service(
    clear_asset_db=False,
    db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
)
```

You can change `db_path` when getting asset service if you have saved asset database to another location.
By default, it is using a preconfigured database located at `data/assets.sqlite`


## Ingest market data

Execute market data ingestion and wait for it to finish:

```python
await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",
        bundle_name="yahoo_finance_daily_data",
        data_bundle_source=market_data_bundle_source,
        data_frequency=data_frequency,
        asset_service=asset_service
    )
```

Data is now ready and you can run backtest by specifying `yahoo_finance_daily_data` as bundle name!

You can find the full working code below:

```python
import asyncio
import datetime
import logging
import pathlib

from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.data.data_sources.yahoo_finance_data_source import YahooFinanceDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_data_yahoo_finance():
    symbols = ["VOO", "META", "AAPL", "AMZN", "NFLX", "GOOGL", "VXX"]

    start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(year=2025, month=8, day=30, tzinfo=datetime.timezone.utc)
    market_data_bundle_source = YahooFinanceDataSource(maximum_threads=1)
    data_frequency = datetime.timedelta(days=1)

    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )
    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",
        bundle_name="yahoo_finance_daily_data",
        data_bundle_source=market_data_bundle_source,
        data_frequency=data_frequency,
        asset_service=asset_service
    )


if __name__ == "__main__":
    configure_logging(level=logging.ERROR, file_name="mylog.log")
    asyncio.run(ingest_data_yahoo_finance())
```

Script is also available in the `examples` directory, named `ingest_data_yahoo_finance.py`.