"""Require everything at once: a cluster of buyers, and one of them an officer.

The last rule in the suite, and the one that tests whether stacking conditions helps. `i01` wants
three insiders inside a fortnight; `i02` wants the CEO or CFO. This wants both on the same name.

There are two ways that turns out, and both are informative. If the strictest filter beats its two
components, the conditions carry independent information and combining them concentrates it. If it
loses to them, the filter has cut the sample past the point where anything survives -- which is the
usual outcome of stacking conditions on alternative data, and the reason a suite like this should
always include one.

Expect very few names: cluster buys fire on 3% of issuer-days and CEO/CFO purchases on 1.3%, so the
intersection is rare and the book is concentrated.

"""
import datetime

from examples.huggingface.insider import mount_features
from examples.huggingface.hf_config import INSIDER10_UNIVERSE
from examples.huggingface.playbook import any_of, equities, hold, show_once

from ziplime.api import date_rules
from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"window": "insider10",
                 "description": "Require a buying cluster and an officer among the buyers"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=120)
FIELDS = ["is_cluster_buy", "n_ceo_buys", "n_cfo_buys"]


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=FIELDS, data_source=context.source)

    clustered = any_of(window, lambda r: r["is_cluster_buy"])
    officers = any_of(window, lambda r: (r["n_ceo_buys"] or 0) + (r["n_cfo_buys"] or 0) > 0)
    for sid in clustered & officers:
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"cluster and officer together, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
