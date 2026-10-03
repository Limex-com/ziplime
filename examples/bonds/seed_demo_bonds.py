"""Write the demo bond universe into data/assets.sqlite.

Run this once before the bond examples:

    python examples/bonds/seed_demo_bonds.py

The issues it writes are synthetic and marked as such -- ``ZL`` tickers, ``DEMOBOND`` identifiers.
They exist so the examples and the end-to-end tests can exercise a whole bond life (coupons,
amortization, an offer window, redemption at maturity) without a vendor token. A connector that
supplies real bond reference data writes into the same database, and the two sets sit side by
side.

Re-running is safe: bonds are identified by ``(identifier, ticker)`` and events by
``(bond, type, date)``, so nothing is duplicated.
"""
import asyncio
import logging

from bond_config import ASSET_DB_PATH

from ziplime.core.ingest_data import get_asset_service
from ziplime.data.data_sources.demo_bonds import build_demo_bond_universe, demo_exchange
from ziplime.utils.logging_utils import configure_logging


async def seed_demo_bonds():
    asset_service = get_asset_service(db_path=ASSET_DB_PATH, clear_asset_db=False)

    await asset_service.save_exchanges(exchanges=[demo_exchange()])
    assets_import = build_demo_bond_universe()
    await asset_service.import_assets(assets_import=assets_import)

    print(f"Seeded {len(assets_import.bonds)} demo bonds "
          f"and {len(assets_import.bond_events)} schedule events into {ASSET_DB_PATH}\n")
    for bond in assets_import.bonds:
        events = [e for e in assets_import.bond_events if e.asset is bond]
        coupons = [e for e in events if e.event_type.value == "COUPON"]
        amortizations = [e for e in events if e.event_type.value == "AMORTIZATION"]
        offers = [e for e in events if e.event_type.value == "OFFER"]
        print(f"  {bond.asset_name:6s} {bond.start_date} -> {bond.maturity_date} "
              f"face={bond.face_value:>7.0f} coupon={bond.coupon_rate:.2%} "
              f"x{bond.coupon_frequency}/yr  "
              f"coupons={len(coupons)} amortizations={len(amortizations)} offers={len(offers)}")

    await asset_service._asset_repository.engine.dispose()


if __name__ == "__main__":
    configure_logging(level=logging.WARNING, file_name="mylog.log")
    asyncio.run(seed_demo_bonds())
