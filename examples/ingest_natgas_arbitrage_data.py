"""Ingest natural gas futures from both venues for the cross-venue arbitrage example.

MOEX FORTS lists a natural gas future, and Finam also mirrors the NYMEX Henry Hub contract. Both
track the same underlying and both are quoted in dollars, so the basis between them is a real
number -- but they are different contracts: 100 MMBtu against 10 000, with different expiry
calendars. They go into their own bundle, separate from the rouble-quoted examples.
"""
import asyncio
import datetime
import logging

from finam_futures_config import (
    ASSET_DB_PATH, NATGAS_BUNDLE_NAME, NATGAS_END, NATGAS_ROOT_SYMBOLS, NATGAS_START,
    TRADING_CALENDAR, require_finam_secret,
)

from ziplime.assets.domain.asset_type import AssetType
from ziplime.core.ingest_data import get_asset_service, ingest_assets, ingest_market_data
from ziplime.utils.bundle_utils import get_asset_data_source, get_market_data_source
from ziplime.utils.logging_utils import configure_logging


async def ingest_natgas_arbitrage_data():
    require_finam_secret()
    asset_data_source = get_asset_data_source("finam")
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    exchanges = await asset_data_source.get_exchanges(root_symbols=NATGAS_ROOT_SYMBOLS)
    await asset_service.save_exchanges(exchanges=exchanges)
    assets_import = await asset_data_source.get_assets(
        exchanges=exchanges, root_symbols=NATGAS_ROOT_SYMBOLS,
        start_date=NATGAS_START, end_date=NATGAS_END)
    await asset_service.import_assets(assets_import=assets_import)

    contracts = []
    for root_symbol in NATGAS_ROOT_SYMBOLS:
        contracts.extend(await asset_service.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol))
    contracts = [c for c in contracts
                 if c.asset.expiration_date >= NATGAS_START and c.start_date <= NATGAS_END]
    print(f"Ingesting {len(contracts)} natural gas contracts across "
          f"{len({c.mic for c in contracts})} venues")

    await ingest_market_data(
        start_date=datetime.datetime.combine(NATGAS_START, datetime.time.min),
        end_date=datetime.datetime.combine(NATGAS_END, datetime.time.min),
        symbols=[c.symbol for c in contracts],
        # One calendar for both venues: a spread can only be traded when both are open anyway, so
        # the Moscow calendar is the binding one. US-only sessions are not represented.
        trading_calendar=TRADING_CALENDAR,
        bundle_name=NATGAS_BUNDLE_NAME,
        data_bundle_source=get_market_data_source("finam", assets=contracts),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        asset_type=AssetType.FUTURES_CONTRACT,
        forward_fill_missing_ohlcv_data=True,
    )


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_natgas_arbitrage_data())
