"""Hold the universe, but be flat into a company's expected results.

The other three strategies here buy *because* of a release. This one avoids being in the name when
one is due, which is what a book that has no view on the outcome should want: an earnings print is
a scheduled jump in variance with no expected return attached to it.

**And it needs a date this dataset does not have.** The calendar records announcements when the
8-K was accepted -- that is what makes it point-in-time -- so nothing in it says a company *will*
report next Tuesday. The date has to be predicted, and the honest way to say how well is to
measure it. On the 12 311 consecutive release gaps in this universe:

    median gap                 91 days
    p10 / p25 / p75 / p90      57 / 79 / 96 / 111
    44.3% of gaps              between 85 and 97 days

Predicting "the last release plus 91 days" therefore has a **median absolute error of 7 days** and
lands within a week only 56.1% of the time. That is the whole difficulty of the strategy in one
number: the window has to be wide enough to cover a guess that poor, and a wide window means
sitting out a large part of every quarter. Whether the variance avoided is worth the return given
up is what the run answers -- read it against `f00`, which holds the same 230 names on the same
decade and never steps aside.

A narrower window would flatter this by pretending the prediction is better than it is.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from earnings import mount, next_expected  # noqa: E402
from hf_config import EARNINGS_UNIVERSE  # noqa: E402
from playbook import equities, hold  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "earnings",
                 "description": "Hold the universe except names whose results are about due"}

#: Days before and after the expected release to stay out. Asymmetric because the prediction is
#: asymmetric: a company that has not reported yet will, whereas one that reported early is done.
LEAD = datetime.timedelta(days=7)
LAG = datetime.timedelta(days=3)


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, EARNINGS_UNIVERSE)
    context.source = await mount(context)
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    # The newest release visible per instrument, whose `date` is its acceptance instant. A name
    # that has never reported inside the window is absent, and absent means held: there is no
    # prediction to act on.
    latest = await data.current(assets=context.universe, fields=["fiscal_period"],
                                data_source=context.source)

    due = set()
    for row in latest.iter_rows(named=True):
        expected = next_expected(row["date"].date())
        if expected - LEAD <= today <= expected + LAG:
            due.add(row["sid"])

    await hold(context, data, [asset.sid for asset in context.universe if asset.sid not in due])
