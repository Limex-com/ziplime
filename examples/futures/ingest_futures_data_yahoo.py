"""Ingest real Yahoo Finance futures chains: the contracts, their specifications, then the bars.

    python examples/futures/ingest_futures_data_yahoo.py

No credentials. Yahoo carries **individual dated contracts** -- ``ESZ26.CME``, ``CLX26.NYM`` --
each with its own expiration date and its own three-year price history, so what gets ingested here
is a genuine term structure rather than a single stitched series.

The chain has to be *discovered*: Yahoo has no endpoint that lists one. Candidate tickers are
generated from each root's listing cycle and probed, and the ones that answer with prices are the
live chain. Contracts that have already expired are removed by Yahoo, so the chain runs forward
from today rather than back through history -- see ``futures_config`` for what that means for a
backtest window.

What Yahoo does not publish at all is the contract specification. The multiplier, the number that
turns a quote into money, comes from a table in ``ziplime/data/data_sources/yahoo/yahoo_futures.py``
transcribed from the exchanges' published terms.
"""
import asyncio
import datetime
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from futures_config import (  # noqa: E402
    ASSET_DB_PATH, END, MONTHS_AHEAD, ROOT_SYMBOLS, START, TRADING_CALENDAR,
)

from ziplime.assets.domain.asset_type import AssetType  # noqa: E402
from ziplime.assets.entities.asset_symbol import AssetSymbol  # noqa: E402
from ziplime.core.ingest_data import get_asset_service, ingest_market_data  # noqa: E402
from ziplime.utils.bundle_utils import get_asset_data_source, get_market_data_source  # noqa: E402
from ziplime.utils.logging_utils import configure_logging  # noqa: E402

BUNDLE_NAME = "yahoo_futures_daily"


async def ingest_futures_data_yahoo():
    asset_data_source = get_asset_data_source("yahoo")
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    exchanges = await asset_data_source.get_futures_exchanges(root_symbols=ROOT_SYMBOLS)
    await asset_service.save_exchanges(exchanges=exchanges)

    print(f"Discovering chains for {', '.join(ROOT_SYMBOLS)} "
          f"({MONTHS_AHEAD} months ahead); this probes Yahoo per candidate contract\n")
    assets_import = await asset_data_source.get_futures(root_symbols=ROOT_SYMBOLS,
                                                        months_ahead=MONTHS_AHEAD)
    await asset_service.import_assets(assets_import=assets_import)

    for root in assets_import.futures_roots:
        chain = [c for c in assets_import.futures if c.root_symbol == root.root_symbol]
        print(f"{root.root_symbol}  {root.description}  {root.exchange.mic}  "
              f"multiplier={root.multiplier:,.0f}  tick={root.tick_size:g}  "
              f"tick value={root.tick_size * root.multiplier:,.2f}  "
              f"{root.settlement_type.value}  ({len(chain)} contracts)")
        for contract in chain:
            print(f"    {contract.asset_name:14s} expires {contract.expiration_date}  "
                  f"last tradable session {contract.auto_close_date}")
        print()

    listings = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol=contract.asset_name, mic=None)
                 for contract in assets_import.futures],
        asset_type=AssetType.FUTURES_CONTRACT)
    listings = [listing for listing in listings if listing is not None]

    print(f"Ingesting bars for {len(listings)} contracts")
    await ingest_market_data(
        start_date=datetime.datetime.combine(START, datetime.time.min),
        end_date=datetime.datetime.combine(END, datetime.time.min),
        symbols=[listing.symbol for listing in listings],
        trading_calendar=TRADING_CALENDAR,
        bundle_name=BUNDLE_NAME,
        data_bundle_source=get_market_data_source("yahoo", assets=listings),
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service,
        forward_fill_missing_ohlcv_data=True,
        # Exact, rather than resolved by name: a ticker is unique only within an asset class.
        assets=listings,
        asset_type=AssetType.FUTURES_CONTRACT,
    )
    await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(ingest_futures_data_yahoo())
