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
    end_date = datetime.datetime(year=2025, month=10, day=27, tzinfo=datetime.timezone.utc)
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
