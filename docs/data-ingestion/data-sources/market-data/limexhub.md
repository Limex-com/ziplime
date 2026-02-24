## Create an account on Limex website

To get access to data from LimexHub you need to have the LimexHub account (created on https://limex.com/) and API key
added
to your account.

## Define symbols, start/end date and data frequency
```python
symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]
start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
end_date = datetime.datetime(year=2025, month=2, day=27, tzinfo=datetime.timezone.utc)
data_frequency = datetime.timedelta(minutes=1)
```

## Initialize client

### Initialize market data source instance
`LimexHubDataSource` instance can be created by providing parameters directly to constructor

```python
market_data_bundle_source = LimexHubDataSource(limex_api_key="YOUR_API_KEY",
                                            maximum_threads=NUMBER_OF_THREDS_YOU_WANT_TO_USE)
```

or by adding next environment variables

- `LIMEX_API_KEY=YOUR_API_KEY`
- `LIMEX_MAXIMUM_THREADS=NUMBER_OF_THREDS_YOU_WANT_TO_USE`

and initializing it using environment variables:

```python
market_data_bundle_source = LimexHubDataSource.from_env()
```

### Initialize fundamental data source instance (optional)

LimexHub has also useful fundamental data that you can use during backtesting.


`LimexHubFundamentalDataSource` instance can be created by providing parameters directly to constructor

```python
data_bundle_source = LimexHubFundamentalDataSource(limex_api_key="YOUR_API_KEY",
                                            maximum_threads=NUMBER_OF_THREDS_YOU_WANT_TO_USE)
```

or by adding next environment variables

- `LIMEX_API_KEY=YOUR_API_KEY`
- `LIMEX_MAXIMUM_THREADS=NUMBER_OF_THREDS_YOU_WANT_TO_USE`

and initializing it using environment variables:

```python
data_bundle_source = LimexHubFundamentalDataSource.from_env()
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
        trading_calendar="NYSE",  # LimexHub contains data for NYSE so we use that calendar
        bundle_name="limex_us_minute_data", # You will use this name later in backtesting
        data_bundle_source=market_data_bundle_source,
        data_frequency=data_frequency,
        asset_service=asset_service
    )
```

## Ingest fundamental data (optional)

Execute fundamental data ingestion and wait for it to finish:

```python
await ingest_custom_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",
        bundle_name="limex_us_fundamental_data",
        data_bundle_source=data_bundle_source,
        data_frequency="1mo", # Get monthly data
        data_frequency_use_window_end=True,
        asset_service=asset_service
    )
```

Data is now ready and you can run backtest!

You can find the full working code below:

```python
import asyncio
import datetime
import pathlib
import logging

from ziplime.core.ingest_data import get_asset_service, ingest_market_data, ingest_custom_data
from ziplime.data.data_sources.limex_hub_fundamental_data_source import LimexHubFundamentalDataSource
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_data_limex_hub():
    """
    Ingests market and fundamental data from Limex Hub for specified financial symbols.

    Fetches both market and fundamental data for a predefined
    set of symbols within a given date range. It utilizes Limex Hub's data sources to
    ingest the data using configured asset services and schedules. The ingested data
    is stored in bundles with specified configurations for further processing or analysis.
    """

    # STEP 1: Define symbols, date range and frequency of the data that we are going to ingest
    symbols = ["META", "AAPL", "AMZN", "NFLX", "GOOGL"]
    start_date = datetime.datetime(year=2025, month=1, day=1, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(year=2025, month=2, day=27, tzinfo=datetime.timezone.utc)
    data_frequency = datetime.timedelta(minutes=1)
    # STEP 2: Initialize market data source and data bundle source - LimexHub
    market_data_bundle_source = LimexHubDataSource.from_env()
    data_bundle_source = LimexHubFundamentalDataSource.from_env()
    # STEP 3: Initialize asset service. Default asset database is used
    asset_service = get_asset_service(
        clear_asset_db=False,
        db_path=str(pathlib.Path(__file__).parent.parent.resolve().joinpath("data", "assets.sqlite"))
    )

    # STEP 4: Ingest market data from limex hub
    await ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",  # LimexHub contains data for NYSE so we use that calendar
        bundle_name="limex_us_minute_data",
        data_bundle_source=market_data_bundle_source,
        data_frequency=data_frequency,
        asset_service=asset_service
    )

    # STEP 5: ingest fundamental data from limex hub
    await ingest_custom_data(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        trading_calendar="NYSE",
        bundle_name="limex_us_fundamental_data",
        data_bundle_source=data_bundle_source,
        data_frequency="1mo",
        data_frequency_use_window_end=True,
        asset_service=asset_service
    )


if __name__ == "__main__":
    configure_logging(level=logging.ERROR, file_name="mylog.log")
    asyncio.run(ingest_data_limex_hub())
```

Script is also available in the `examples` directory, named `ingest_data_limex_hub.py`.