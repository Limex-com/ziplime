"""Buy where many *different* insiders have been buying, using the 30-day unique-buyer count.

Breadth, not clustering. `is_cluster_buy` demands three distinct buyers inside a 14-day window --
a tight, rare event. This asks a looser question over a longer horizon: how many separate people
at this company have bought in the last month, whether or not they did it at the same time.

If the cluster flag is capturing something real, a looser version of the same idea should capture
a weaker version of it. If `i01` beats this and `i00`, the tightness is doing the work.

"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from insider import mount_features  # noqa: E402
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from playbook import any_of, equities, every, hold, show_once  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names with several distinct insider buyers over the past 30 days"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=90)
FIELDS = ['n_unique_buyers_30d']


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}


@every(days=7)
async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)

    for sid in any_of(window, lambda r: (r["n_unique_buyers_30d"] or 0) >= 3):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"first signal, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
