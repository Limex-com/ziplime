"""Shared settings for the cross-asset examples: equities and bonds in one portfolio."""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

TRADING_CALENDAR = "XNYS"

#: Equities come from Yahoo Finance, which needs no credentials.
EQUITY_TICKERS = ["JNJ"]
EQUITY_MIC = "XNYS"

#: Bonds come from the synthetic demo universe seeded by examples/bonds/seed_demo_bonds.py.
#: No vendor publishes a coupon schedule for free, so a bond example that runs without
#: credentials has to generate one -- see ziplime.data.data_sources.demo_bonds.
BOND_TICKERS = ["ZLB26"]
BOND_MIC = "XNYS"

#: Window both instruments have data for.
START = datetime.date(2023, 6, 1)
END = datetime.date(2026, 5, 29)

STARTING_CASH = 1_000_000.0
