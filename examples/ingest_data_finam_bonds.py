"""Ingest daily OHLCV bars for MOEX bonds into a ziplime bundle.

Run ``ingest_assets_data_finam_bonds.py`` first: bars are keyed by the sid it assigns.

Bond bars are quoted the way the exchange quotes them -- as a **percentage of face value**, so a
close of 66.5 on a 1000-rouble nominal is 665 roubles. Nothing here converts them; the engine does,
against the nominal outstanding on the day, which is why the coupon schedule has to be ingested
before the prices mean anything.
"""
import argparse
import asyncio
import datetime
import logging

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service, ingest_market_data
from ziplime.utils.bundle_utils import get_market_data_source
from ziplime.utils.logging_utils import configure_logging

from examples.bonds.bond_config import (
    ASSET_DB_PATH, BOND_BUNDLE_NAME, INGEST_END, INGEST_START, MIC, TRADING_CALENDAR,
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("tickers", nargs="*",
                        help="Bond tickers to ingest bars for. Defaults to every stored bond.")
    parser.add_argument("--bundle", default=BOND_BUNDLE_NAME)
    parser.add_argument("--start", type=datetime.date.fromisoformat, default=INGEST_START)
    parser.add_argument("--end", type=datetime.date.fromisoformat, default=INGEST_END)
    return parser.parse_args()


async def ingest_data_finam_bonds(args):
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    if args.tickers:
        listings = [await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=MIC), asset_type=AssetType.BOND)
            for ticker in args.tickers]
        missing = [t for t, listing in zip(args.tickers, listings) if listing is None]
        if missing:
            raise SystemExit(f"Not in the asset database: {', '.join(missing)}. "
                             f"Run ingest_assets_data_finam_bonds.py for them first.")
    else:
        listings = await asset_service.get_all_bond_listings(mic=MIC)

    if not listings:
        raise SystemExit("No bonds in the asset database. "
                         "Run ingest_assets_data_finam_bonds.py first.")

    symbols = [listing.symbol for listing in listings]
    print(f"Ingesting bars for {len(symbols)} bonds: {', '.join(symbols)}")

    await ingest_market_data(
        start_date=datetime.datetime.combine(args.start, datetime.time.min),
        end_date=datetime.datetime.combine(args.end, datetime.time.min),
        symbols=symbols,
        trading_calendar=TRADING_CALENDAR,
        bundle_name=args.bundle,
        # Passing the listings keeps each request inside the bond's own lifetime.
        data_bundle_source=get_market_data_source("finam", assets=listings, default_mic=MIC),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        forward_fill_missing_ohlcv_data=True,
        asset_type=AssetType.BOND,
    )
    await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_data_finam_bonds(parse_args()))
