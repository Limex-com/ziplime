## Create an account on Limex website

To get access to data from LimexHub you need to have the LimexHub account (created on https://limex.com/) and API key
added
to your account.

## Initialize client

`LimexHubAssetDataSource` instance can be created by providing parameters directly to constructor

```python
asset_data_source = LimexHubAssetDataSource(limex_api_key="YOUR_API_KEY",
                                            maximum_threads=NUMBER_OF_THREDS_YOU_WANT_TO_USE)
```

or by adding next environment variables

- `LIMEX_API_KEY=YOUR_API_KEY`
- `LIMEX_MAXIMUM_THREADS=NUMBER_OF_THREDS_YOU_WANT_TO_USE`

and initializing it using environment variables:

```python
asset_data_source = LimexHubAssetDataSource.from_env()
```
Number of threads is optional, it's used to speed up fetching of asset data by executing multiple
API requests at once. If not specified, the maximum number will be set to the number of CPU cores.

## Configure asset service

```python
asset_service = get_asset_service(
    clear_asset_db=True,
)
```

You can optionally specify `db_path` when getting asset service if you want to use custom location for a sqlite
database, by default, it is stored in `~/.ziplime/assets.sqlite.`

```python
asset_service = get_asset_service(
    clear_asset_db=True,
    db_path="/tmp/assets_db.sqlite"
)
```

## Ingest assets

Execute asset ingestion and wait for it to finish:

```python
await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)
```

Optionally, ingest also symbol universes data

```python
await ingest_symbol_universes(asset_service=asset_service, asset_data_source=asset_data_source)
```

Now your assets are ready!

You can find the full working code below:

```python
import asyncio
import logging

from ziplime.core.ingest_data import get_asset_service, ingest_symbol_universes, ingest_assets
from ziplime.data.data_sources.limex_hub_asset_data_source import LimexHubAssetDataSource
from ziplime.utils.logging_utils import configure_logging


async def ingest_assets_data_limex_hub():
    asset_data_source = LimexHubAssetDataSource.from_env()
    asset_service = get_asset_service(
        clear_asset_db=True,
    )
    await ingest_assets(asset_service=asset_service, asset_data_source=asset_data_source)
    await ingest_symbol_universes(asset_service=asset_service, asset_data_source=asset_data_source)


if __name__ == "__main__":
    configure_logging(level=logging.ERROR, file_name="mylog.log")
    asyncio.run(ingest_assets_data_limex_hub())

```

Script is also available in the `examples` directory, named `ingest_assets_data_limex_hub.py`.