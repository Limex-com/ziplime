"""Buy where an insider raised their own stake by a large percentage.

Dollar size says how rich the insider is; percentage says how convinced they are. An executive who
already owns ten million dollars of stock and buys another hundred thousand has changed nothing
about their exposure. One who raises their holding by half has.

`max_holding_change_pct` is the dataset's own before-and-after computation, and it is thin: the
publisher emits it only where the position logic is unambiguous, which is 26% of transaction rows
and about 3% of issuer-days. This is the smallest sample in the suite, and its result should be
read with that in mind.

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

STRATEGY_INFO = {"window": "insider10", "description": "Hold names where an insider raised their own position by a large share"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=90)
FIELDS = ['max_holding_change_pct']


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}


@every(days=7)
async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)

    for sid in any_of(window, lambda r: (r["max_holding_change_pct"] or 0.0) >= 0.25):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"first signal, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
