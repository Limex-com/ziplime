"""The same rule and the same information, executed thirty minutes before the close.

Paired with `m02`, which executes the identical decision thirty minutes after the open. Both
read the disclosures at the first bar of the session, so the *information* is the same in both;
only the minute the orders go out differs. Whatever separates their returns is the time of day and
nothing else.

That pairing is the intraday version of a question the daily strategies raised and could not
answer. Moving the weekly rebalance from Tuesday to Monday -- the same 548 rebalances, the same
signal -- moved `i04` by 171 percentage points across the five weekdays. If a decade of returns
turns on which day of the week the orders go out, it is worth knowing what the hour does.

Twenty sessions of five-minute bars is a demonstration, not evidence, and Yahoo deletes intraday
history as it ages so the window moves with the calendar. Read the pair against each other, and
read neither against zero.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from intraday import INTRADAY_UNIVERSE, mount_sales, note_sales, unblocked  # noqa: E402
from playbook import equities, hold  # noqa: E402

from ziplime.api import date_rules, time_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "intraday-5m",
                 "description": "Avoid names insiders sold, acting 30 minutes before the close"}


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INTRADAY_UNIVERSE)
    context.source = await mount_sales(context)
    context.blocked = {}
    # Two rules, the classic shape: one decides, the other trades. Reading at the first bar keeps
    # the information identical to `m03`, which trades near the open on the same decision.
    context.schedule_function(look, date_rules.every_day(), time_rules.market_open(minutes=1))
    context.schedule_function(trade, date_rules.every_day(), time_rules.market_close(minutes=30))


async def look(context: TradingAlgorithm, data: BarData):
    await note_sales(context, data)


async def trade(context: TradingAlgorithm, data: BarData):
    await hold(context, data, unblocked(context))
