"""Shared settings for the bond examples."""
import datetime
from pathlib import Path

#: The asset database the bond examples read and write.
ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

#: MOEX main market, where rouble bonds trade.
MIC = "MISX"

#: exchange_calendars name for the Moscow Exchange.
TRADING_CALENDAR = "XMOS"

#: The demo issues seeded by seed_demo_bonds.py -- synthetic, for running without a token.
DEMO_TICKERS = ["ZLB26", "ZLZ26", "ZLA27", "ZLO27", "ZLS25"]

#: Real MOEX issues ingested by ingest_assets_data_finam_bonds.py, chosen to span the cases:
#: two plain OFZ bullets of different duration, an amortizing OFZ, two amortizing corporates
#: (both floaters) and a dollar-denominated eurobond.
REAL_TICKERS = ["SU26238RMFS4", "SU26212RMFS9", "SU46020RMFS2",
                "RU000A0JRU20", "RU000A0JR4U9", "XS0114288789"]

#: Bundle the real bond bars land in.
BOND_BUNDLE_NAME = "finam_bonds_daily"

#: Window for the real-data ingest and examples.
INGEST_START = datetime.date(2024, 1, 1)
INGEST_END = datetime.date(2026, 8, 1)

#: Window the strategy examples run over. Every demo issue is alive across it, ZLS25 matures
#: inside it, and ZLA27 pays two amortization instalments during it.
EXAMPLE_START = datetime.date(2023, 6, 1)
EXAMPLE_END = datetime.date(2026, 5, 29)

STARTING_CASH = 1_000_000.0
