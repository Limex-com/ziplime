"""Shared settings for the 0DTE option examples.

**The option prices here are synthetic.** They are generated from SPY's real bars and a
volatility model, and they exist so that option machinery can be built and tested against
something with the right shape. They are not a market, and no number produced from them describes
what a strategy would have earned -- see
:data:`ziplime.data.data_sources.options.synthetic.SYNTHETIC_DATA_WARNING`.
"""
import datetime
from pathlib import Path

ASSET_DB_PATH = str(Path(Path(__file__).parent.parent.parent, "data", "assets.sqlite").absolute())

#: SPY on NYSE Arca. The underlying is real; everything written on it is not.
UNDERLYING = ("SPY", "ARCX")
TRADING_CALENDAR = "XNYS"

#: 0DTE needs intraday bars, and Yahoo keeps only a short window of them -- seven days at one
#: minute, sixty at five. The window therefore follows the calendar rather than being written
#: down, exactly as in examples/huggingface/intraday.py, and these examples demonstrate a
#: mechanism rather than reproduce a number.
EMISSION_RATE = datetime.timedelta(minutes=5)
SESSIONS = 5

STARTING_CASH = 100_000.0

#: The strike grid, in the shape OPRA actually lists for SPY: a dollar apart near the money, five
#: apart in the wings. Measured from the live reference feed -- a real 0DTE chain on a 764 spot had
#: 155 strikes from 550 to 950. Narrowed here because every strike is a contract to store and
#: price, and nothing trades 200 points out on the day it expires.
NEAR_STEP, NEAR_REACH = 1.0, 20.0
FAR_STEP, FAR_REACH = 5.0, 60.0
