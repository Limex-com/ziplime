"""Buy where the chief executive or chief financial officer bought on the open market.

Not every insider knows the same things. A director attends eight board meetings a year; the CEO
and CFO see the numbers as they form. The literature has long held that purchases by the two
officers closest to the accounts are the most informative subset of Form 4 filings, and this tests
exactly that against `i01`, which counts any insider.

The filter is narrow -- 3 875 of 297 066 issuer-days in 2022 carry a CEO or CFO purchase -- so the
book is small and turns over slowly.

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

STRATEGY_INFO = {"window": "insider10", "description": "Hold names the CEO or CFO bought on the open market"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=90)
FIELDS = ['n_ceo_buys', 'n_cfo_buys']


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}


@every(days=7)
async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)

    for sid in any_of(window, lambda r: (r["n_ceo_buys"] or 0) + (r["n_cfo_buys"] or 0) > 0):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"first signal, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
