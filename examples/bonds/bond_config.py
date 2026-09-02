"""Shared settings for the bond examples."""
import datetime
from pathlib import Path

#: The asset database the bond examples read and write.
ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

#: The venue the demo issues trade on. They are synthetic, so this only has to agree with the
#: calendar below and with whatever equities they are held alongside.
MIC = "XNYS"

#: exchange_calendars name for the New York Stock Exchange.
TRADING_CALENDAR = "XNYS"

#: The demo issues seeded by seed_demo_bonds.py -- synthetic, for running without a token.
DEMO_TICKERS = ["ZLB26", "ZLZ26", "ZLA27", "ZLO27", "ZLS25"]



#: Window the strategy examples run over. Every demo issue is alive across it, ZLS25 matures
#: inside it, and ZLA27 pays two amortization instalments during it.
EXAMPLE_START = datetime.date(2023, 6, 1)
EXAMPLE_END = datetime.date(2026, 5, 29)

STARTING_CASH = 1_000_000.0
