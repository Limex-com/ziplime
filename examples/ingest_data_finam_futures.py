"""Ingest daily OHLCV bars for MOEX FORTS futures into a ziplime bundle."""
import asyncio
import datetime
import logging

from finam_futures_config import (
    ASSET_DB_PATH, BUNDLE_NAME, INGEST_END, INGEST_START, MIC, ROOT_SYMBOLS, TRADING_CALENDAR,
    require_finam_secret,
)

from ziplime.assets.domain.asset_type import AssetType
from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.utils.bundle_utils import get_market_data_source
from ziplime.utils.logging_utils import configure_logging


async def ingest_data_finam_futures():
    require_finam_secret()
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    # Ingest every contract of the requested chains, expired ones included: a backtest that rolls
    # needs the contracts that were front-month back then, not just the ones listed today.
    # No MIC filter: roots now span venues (MOEX FORTS and the NYMEX contracts Finam mirrors),
    # and each listing already carries its own exchange.
    contracts = []
    for root_symbol in ROOT_SYMBOLS:
        contracts.extend(await asset_service.get_exchange_futures_contracts_by_root(
            root_symbol=root_symbol))
    symbols = [contract.symbol for contract in contracts]
    if not symbols:
        raise SystemExit("No futures contracts in the asset database. "
                         "Run ingest_assets_data_finam.py first.")
    print(f"Ingesting {len(symbols)} contracts: {', '.join(symbols)}")

    await ingest_market_data(
        start_date=datetime.datetime.combine(INGEST_START, datetime.time.min),
        end_date=datetime.datetime.combine(INGEST_END, datetime.time.min),
        symbols=symbols,
        trading_calendar=TRADING_CALENDAR,
        bundle_name=BUNDLE_NAME,
        # Passing the listings keeps each request inside the contract's own lifetime.
        # Each listing carries its own MIC, so the source only needs a fallback for bare symbols.
        data_bundle_source=get_market_data_source("finam", assets=contracts, default_mic=MIC),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        forward_fill_missing_ohlcv_data=True,
        asset_type=AssetType.FUTURES_CONTRACT,
    )


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_data_finam_futures())
