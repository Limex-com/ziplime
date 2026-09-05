"""Shared settings for the Hugging Face point-in-time dataset examples."""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

TRADING_CALENDAR = "XNYS"
BUNDLE_NAME = "hf_equities_daily"

#: Equities to price the strategies with. Yahoo Finance, no credentials.
#:
#: These five are the names Congress discloses most often, which is what makes the examples show
#: anything: a dataset row that lands on a ticker the backtest does not hold is invisible.
EQUITY_TICKERS = ["MSFT", "NVDA", "AAPL", "AMZN", "META"]
#: All five list on Nasdaq Global Select. Naming the MIC matters: META is also listed on ARCX,
#: and resolving by ticker alone would have to pick one of the two.
EQUITY_MIC = "XNGS"

#: The wider universe the congressional strategies trade, as (ticker, MIC) pairs.
#:
#: Every one is named with its venue on purpose. A bare ticker is not unique in this database:
#: `T` resolves to a Moscow listing, `META` to both Nasdaq and NYSE Arca. Resolving by name alone
#: picks one silently, and not necessarily the one the price bundle holds.
CONGRESS_UNIVERSE = [
    ("AAPL", "XNGS"), ("NVDA", "XNGS"), ("MSFT", "XNGS"), ("AMZN", "XNGS"),
    ("GOOGL", "XNGS"), ("TSLA", "XNGS"), ("META", "XNGS"),
    ("V", "XNYS"), ("DIS", "XNYS"), ("JPM", "XNYS"), ("BAC", "XNYS"),
    ("PFE", "XNYS"), ("CRM", "XNYS"), ("PG", "XNYS"),
]

#: Window for the congressional strategies. Starts in 2016: the House catalog is thin before then
#: -- roughly 60% of 2014 filings were scanned paper that no extractor has read.
CONGRESS_START = datetime.date(2016, 1, 4)
CONGRESS_END = datetime.date(2026, 8, 31)

#: The committee whose members `h04` follows. House Armed Services is the most active in this
#: data: 20 449 transaction reports from its members, against 6 711 for Financial Services.
COMMITTEE_ID = "HSAS"

#: Window for the congress examples. The dataset runs 2012-01-25 to 2026-08-24.
START = datetime.date(2023, 1, 1)
END = datetime.date(2026, 8, 31)

#: Window for the insider examples. That dataset stops at 2016-03-02, so its strategies run
#: earlier; mounting it over the window above would fetch partitions holding nothing.
INSIDER_START = datetime.date(2012, 1, 1)
INSIDER_END = datetime.date(2014, 12, 31)

STARTING_CASH = 1_000_000.0

#: Datasets these examples mount, pinned so the numbers in the README stay reproducible.
#:
#: Passing a commit rather than a branch is the whole point of pinning: these datasets are
#: append-only and grow, so `main` today is not `main` next month, and an unpinned backtest
#: quietly stops being comparable with the one you ran before.
CONGRESS_DATASET = "ZipLime/congress-trading"
CONGRESS_REVISION = "67c335f5207d5190ada5f89803615848dbf52ee0"
INSIDER_DATASET = "ZipLime/insider-trading"
INSIDER_REVISION = "ba0785efcede0b3a13af48dc658a1d39bc87ad1e"
