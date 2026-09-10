"""Buy where an outside director bought, and nobody in management did.

The mirror of `i02`. Directors are not employees: they sit on the board, see the same reports as
the auditors, and have no salary tied to the share price. Whether that makes their purchases more
informative than an officer's, or less, is an open question that this data can answer directly --
`n_director_buys` and `n_ceo_buys`/`n_cfo_buys` are separate columns.

Management purchases are excluded rather than ignored, so the two books do not overlap and the
comparison against `i02` is clean.

"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from insider import mount_features  # noqa: E402
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from playbook import any_of, equities, hold, show_once  # noqa: E402

from ziplime.api import date_rules  # noqa: E402
from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names an outside director bought while management did not"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=90)
FIELDS = ['n_director_buys', 'n_ceo_buys', 'n_cfo_buys']


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)

    for sid in any_of(window, lambda r: (r["n_director_buys"] or 0) > 0
                            and (r["n_ceo_buys"] or 0) + (r["n_cfo_buys"] or 0) == 0):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"first signal, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
