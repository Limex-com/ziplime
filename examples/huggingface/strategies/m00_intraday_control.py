"""Hold all nineteen names equally and read nothing. What `m02` and `m03` are measured against.

Bought once at the first bar and never touched again, so it carries no timing decision of its own
-- which is the point, since the pair it exists for differ only in when they trade. Any return
this makes is what twenty sessions of these particular names gave away for free.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from intraday import INTRADAY_UNIVERSE  # noqa: E402
from playbook import equities, hold  # noqa: E402

from ziplime.api import date_rules, time_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "intraday-5m",
                 "description": "Equal-weight all 19 names, reading no filing -- the control"}


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INTRADAY_UNIVERSE)
    context.schedule_function(buy, date_rules.every_day(), time_rules.market_open(minutes=30))


async def buy(context: TradingAlgorithm, data: BarData):
    # `hold` trades only when the set of names changes, and it never changes here: this places the
    # book on the first session and is a no-op on every one after it.
    await hold(context, data, [asset.sid for asset in context.universe])
