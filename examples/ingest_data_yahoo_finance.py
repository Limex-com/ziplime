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
    data_frequency = datetime.timedelta(days=1)

    market_data_bundle_source = YahooFinanceDataSource(maximum_threads=1)

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
