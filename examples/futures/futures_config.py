"""Shared settings for the futures examples, which run on real Yahoo Finance contract chains."""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

TRADING_CALENDAR = "XNYS"

#: Roots to ingest. Every one is in ziplime.data.data_sources.yahoo.yahoo_futures, which carries
#: the contract specification Yahoo omits -- above all the multiplier.
#:
#: ES gives a quarterly chain of five live contracts; CL a monthly chain of two dozen, which is a
#: deep enough curve to read. Gold and silver are absent on purpose: Yahoo's volume for them is
#: not the real figure, so a volume-share slippage model against it would be meaningless.
ROOT_SYMBOLS = ["ES", "CL", "NG", "ZC"]

#: How far ahead to look for contracts when discovering a chain.
MONTHS_AHEAD = 24

# ---------------------------------------------------------------------------------------------
# Two windows, because the chain Yahoo serves is live rather than historical.
#
# Yahoo **removes a contract once it expires**. So the chain contains only contracts that have
# not settled yet -- and each of those carries its full history, three years deep. Two different
# things follow from that, and conflating them produces a backtest that looks right and is not:
#
#   * Every contract in the chain is real, and their prices are simultaneous and comparable over
#     the whole history. Term structure, calendar spreads and cross-market work are therefore
#     sound across the long window.
#   * But the *front* of the chain today was not the front of the chain in 2023 -- the contracts
#     that were front-month back then are gone. A strategy that holds "the front contract" over
#     the long window would hold a 2026 contract throughout 2023, which no trader did.
#
# So roll behaviour is only genuine near the end of the data, where the chain's front contract
# really is the market's front contract.
# ---------------------------------------------------------------------------------------------

#: Window for curve work -- term structure, spreads, baskets. Every contract is real here.
#:
#: It starts where the *whole* ES chain has bars. Contracts list on their own schedules -- the WTI
#: chain goes back to 2018, while the deferred S&P contracts only began trading in August 2023 --
#: and a window that opens before a contract's first bar puts it on the curve with no price.
START = datetime.date(2023, 9, 1)
END = datetime.date(2026, 9, 2)

#: Window for roll work: recent enough that the front of the chain is the real front month.
#: The October WTI contract (CLV26) stops being the front month inside it, so a roll genuinely
#: happens rather than being simulated.
ROLL_START = datetime.date(2026, 6, 1)
ROLL_END = datetime.date(2026, 9, 2)

STARTING_CASH = 1_000_000.0

#: Margin as a share of notional. Real exchange margin is set per contract and moves; this is a
#: round number that makes the mechanism visible rather than a broker's schedule.
INITIAL_MARGIN_RATE = 0.10
MAINTENANCE_MARGIN_RATE = 0.08

#: Calendar days before a contract's auto close to move to the next one. Deliberately wide: it
#: puts the roll well clear of the delivery window, which a deliverable contract like WTI needs.
ROLL_OFFSET_DAYS = 21
