"""The same drift rule as `e01`, restricted to results released before the open.

`e01` buys on any release that grew revenue. This one buys only the ones whose acceptance
timestamp fell in the `pre_market` window, and its pair does the other half. Nothing else differs.

Why that is worth a strategy rather than a footnote: on daily bars the two are not the same trade.
A release accepted at 09:00 is public before that day's close, so the position opens at the close
of the day the results came out. One accepted at 16:30 is not, and the position opens a full
session later. The signal is identical; the entry is a day apart, and the drift literature says
the first day is where most of the move is.

Splitting this way is only possible because every timestamp here is read from the filing's own
SGML header. EDGAR's bulk feed labels the same field with a `Z` while carrying Eastern local time
for 94% of pre-2021 filings -- four hours of error, which is exactly enough to move a release from
one side of the close to the other and swap these two strategies.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from earnings import growers, mount  # noqa: E402
from hf_config import EARNINGS_UNIVERSE  # noqa: E402
from playbook import equities, hold  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "earnings",
                 "description": "Revenue growth, released before the open only"}

SESSION = "pre_market"
HOLD_FOR = datetime.timedelta(days=60)


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, EARNINGS_UNIVERSE)
    context.source = await mount(context)
    context.opened_on = {}
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    for sid in await growers(context, data, session=SESSION):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}
    await hold(context, data, context.opened_on)
