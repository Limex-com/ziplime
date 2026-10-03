"""Buy on the results, hold through the drift: revenue up on the year, entered when it was public.

Post-earnings announcement drift is the oldest documented anomaly in the calendar literature --
prices keep moving in the direction of the surprise for weeks after the release. Testing it needs
two things this dataset supplies and nothing else here does: the instant the results became
public, and the figures they contained.

There is no consensus estimate in any free source, so there is no surprise against consensus. What
there is, is the figure and the year-ago figure. Growth against the same quarter a year earlier is
the crude version of the signal -- it measures the company rather than the expectation -- and it is
what can be built honestly from public filings.

**Revenue growth, not EPS growth.** Both are in the dataset. EPS is the sharper signal and the
dirtier column: 18 releases in the corpus carry an `eps_diluted` in the millions, a filer having
tagged the total into the per-share field, and a rank puts a number like that at the top of the
book rather than in the middle of it. `earnings.load_releases` discards those, and this still
prefers revenue, which no filer confuses with anything.

The entry bar is the point. A release accepted at 16:30 is not visible to a simulation standing at
that day's 16:00 close, and the adapter enforces that from the acceptance timestamp without the
strategy doing anything: the name is bought at the next session instead. On daily bars that is as
close to the release as it is possible to get.
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
                 "description": "Hold names whose latest results grew revenue year on year"}

#: How long the drift is held for. Sixty days is the span the literature measures it over.
HOLD_FOR = datetime.timedelta(days=60)


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, EARNINGS_UNIVERSE)
    context.source = await mount(context)
    context.opened_on = {}
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    for sid in await growers(context, data):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}
    await hold(context, data, context.opened_on)
