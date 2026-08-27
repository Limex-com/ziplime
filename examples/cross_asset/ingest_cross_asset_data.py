"""Ingest one bundle holding equities, bonds and futures together.

    export FINAM_API_SECRET=tapi_sk_...
    python examples/cross_asset/ingest_cross_asset_data.py

A simulation reads from a single market data source, so a strategy that trades more than one asset
class needs them in **one** bundle.

The listings are resolved here, by class, and handed to the ingest as ``assets``. That matters: a
ticker is unique only *within* an asset class, and this database holds 168 tickers that exist under
two -- ``SiH5@RTSX`` is a futures contract and also, because an equity vendor listed it that way,
an equity. Resolving by name alone would tag the futures bars with the equity's sid, and the
strategy would then find no prices for the contract it was holding.

Reference data has to exist first -- run the equity, bond and futures asset ingests -- because bars
are keyed by the sids those steps assign.
"""
import asyncio
import datetime
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from cross_asset_config import (  # noqa: E402
    ASSET_DB_PATH, BOND_MIC, BOND_TICKERS, BUNDLE_NAME, END, EQUITY_MIC, EQUITY_TICKERS,
    FUTURES_MIC, FUTURES_TICKERS, START, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service, ingest_market_data  # noqa: E402
from ziplime.utils.bundle_utils import get_market_data_source  # noqa: E402
from ziplime.utils.logging_utils import configure_logging  # noqa: E402

#: The classes this bundle spans, in lookup order.
ASSET_TYPES = [AssetType.EQUITY, AssetType.BOND, AssetType.FUTURES_CONTRACT]


async def resolve(asset_service, tickers, mic, asset_type):
    listings = []
    for ticker in tickers:
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic), asset_type=asset_type)
        if listing is None:
            raise SystemExit(
                f"{ticker}@{mic} is not in the asset database as {asset_type.value}. "
                f"Run the reference-data ingest for it first.")
        listings.append(listing)
    return listings


async def ingest_cross_asset_data():
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    listings = (
        await resolve(asset_service, EQUITY_TICKERS, EQUITY_MIC, AssetType.EQUITY)
        + await resolve(asset_service, BOND_TICKERS, BOND_MIC, AssetType.BOND)
        + await resolve(asset_service, FUTURES_TICKERS, FUTURES_MIC, AssetType.FUTURES_CONTRACT)
    )
    print(f"Ingesting {len(listings)} instruments across "
          f"{len({type(listing.asset).__name__ for listing in listings})} asset classes:")
    for listing in listings:
        print(f"  {listing.symbol:16s} {listing.mic:6s} {type(listing.asset).__name__}")

    await ingest_market_data(
        start_date=datetime.datetime.combine(START, datetime.time.min),
        end_date=datetime.datetime.combine(END, datetime.time.min),
        symbols=[listing.symbol for listing in listings],
        trading_calendar=TRADING_CALENDAR,
        bundle_name=BUNDLE_NAME,
        # Each listing carries its own MIC, so one source serves both venues.
        data_bundle_source=get_market_data_source("finam", assets=listings,
                                                 default_mic=EQUITY_MIC),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        forward_fill_missing_ohlcv_data=True,
        # Exact, rather than resolved by name: see the module docstring.
        assets=listings,
        asset_type=ASSET_TYPES,
    )
    await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(ingest_cross_asset_data())
