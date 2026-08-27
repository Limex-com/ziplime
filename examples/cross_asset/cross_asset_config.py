"""Shared settings for the cross-asset examples: equities, bonds and futures in one portfolio."""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

TRADING_CALENDAR = "XMOS"

#: One bundle holds all three classes: a simulation reads from a single market data source, so a
#: cross-asset strategy cannot be split across bundles.
BUNDLE_NAME = "moex_cross_asset_daily"

#: Sberbank ordinary shares on the MOEX main market.
EQUITY_TICKERS = ["SBER"]
EQUITY_MIC = "MISX"

#: A long OFZ bullet, the same issue the bond examples use.
BOND_TICKERS = ["SU26238RMFS4"]
BOND_MIC = "MISX"

#: USD/RUB futures on the MOEX derivatives market. Contracts, not a chain: a cross-asset example
#: is about the asset classes coexisting, not about rolling.
FUTURES_TICKERS = ["SiH5", "SiM5", "SiU5", "SiZ5", "SiH6", "SiM6"]
FUTURES_MIC = "RTSX"

#: Window every instrument above has bars for.
START = datetime.date(2024, 6, 1)
END = datetime.date(2026, 6, 30)

STARTING_CASH = 5_000_000.0
