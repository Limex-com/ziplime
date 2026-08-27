"""Ingest real MOEX bonds and their coupon schedules from the Finam Trade API.

Run this before ingesting bond bars: prices are keyed by the sid this step assigns.

    export FINAM_API_SECRET=tapi_sk_...
    python examples/ingest_assets_data_finam_bonds.py SU26238RMFS4 RU000A105104

Given no tickers it sweeps the whole MOEX bond list, capped by ``--limit``, because MOEX lists tens
of thousands of issues and each one costs at least one calendar request.

What comes back and how far to trust it. A bond's terms are read off its calendar -- both halves
of it, ``/v1/bonds/past`` and ``/v1/bonds/future`` -- because no endpoint states them outright.
Realised coupons are exact, and so are scheduled ones for a fixed-coupon issue. Two caveats:

* **A redeemed issue cannot be ingested.** Both calendar endpoints answer with nothing for an
  archived listing, so there is no schedule, no nominal and no maturity to derive. Only issues that
  are still listed can be ingested -- their history is complete, so backtests over the past work,
  but a universe of already-matured bonds is not obtainable here.
* **A floater's future coupons are published with a rate of zero**, because it has not been fixed.
  They are ingested as dated events that pay nothing, and the ingest warns per issue.
"""
import argparse
import asyncio
import datetime
import logging

from ziplime.core.ingest_data import get_asset_service
from ziplime.data.data_sources.finam.finam_bonds import MISX_MIC
from ziplime.utils.bundle_utils import get_asset_data_source
from ziplime.utils.logging_utils import configure_logging

from examples.bonds.bond_config import ASSET_DB_PATH


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("tickers", nargs="*",
                        help="Bond tickers to ingest, e.g. SU26238RMFS4. Sweeps the venue if omitted.")
    parser.add_argument("--isin", action="append", dest="isins", default=None,
                        help="Select by ISIN instead of ticker. Repeatable.")
    parser.add_argument("--mic", default=MISX_MIC, help="Exchange to ingest from.")
    parser.add_argument("--limit", type=int, default=25,
                        help="Cap on a sweep with no ticker or ISIN given.")
    parser.add_argument("--start", type=datetime.date.fromisoformat, default=None,
                        help="Skip issues that matured before this date (YYYY-MM-DD).")
    parser.add_argument("--end", type=datetime.date.fromisoformat, default=None,
                        help="Skip issues first traded after this date (YYYY-MM-DD).")
    parser.add_argument("--include-matured", action="store_true",
                        help="Also try archived listings. They have no calendar, so this normally "
                             "only spends requests; kept so the behaviour can be re-checked.")
    return parser.parse_args()


async def ingest_bonds(args):
    asset_data_source = get_asset_data_source("finam")
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    exchanges = await asset_data_source.get_bond_exchanges(mic=args.mic)
    await asset_service.save_exchanges(exchanges=exchanges)

    assets_import = await asset_data_source.get_bonds(
        exchanges=exchanges,
        tickers=args.tickers or None,
        isins=args.isins,
        mic=args.mic,
        start_date=args.start,
        end_date=args.end,
        include_matured=args.include_matured,
        # An explicit selection is already a limit; the cap is for the open-ended sweep.
        limit=None if (args.tickers or args.isins) else args.limit,
    )
    await asset_service.import_assets(assets_import=assets_import)

    print(f"Ingested {len(assets_import.bonds)} bonds "
          f"and {len(assets_import.bond_events)} schedule events\n")
    for bond in assets_import.bonds:
        events = [e for e in assets_import.bond_events if e.asset is bond]
        coupons = sum(1 for e in events if e.event_type.value == "COUPON")
        amortizations = sum(1 for e in events if e.event_type.value == "AMORTIZATION")
        offers = sum(1 for e in events if e.event_type.value == "OFFER")
        print(f"  {bond.asset_name:16s} {bond.isin or '':14s} "
              f"{bond.start_date} -> {bond.maturity_date} "
              f"{bond.face_value:>9.2f} {bond.quote_currency} "
              f"{bond.coupon_rate:>7.2%} x{bond.coupon_frequency}/yr "
              f"cp={coupons:<3d} am={amortizations:<3d} of={offers:<2d}"
              f"{' AMORTIZED' if bond.is_amortized else ''}")

    await asset_data_source.aclose()
    await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    configure_logging(level=logging.INFO, file_name="mylog.log")
    asyncio.run(ingest_bonds(parse_args()))
