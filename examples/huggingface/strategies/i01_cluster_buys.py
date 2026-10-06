"""Buy where several insiders bought at once -- the dataset's own `is_cluster_buy`.

The canonical insider anomaly. One executive buys for a dozen reasons: a bonus to deploy, an
ownership guideline to meet, optimism about a house purchase. Three of them buying the same stock
on the open market inside a fortnight is much harder to explain by anything other than a shared
view of the company.

The flag is the publisher's: at least three distinct insiders making open-market purchases in a
trailing 14 calendar-day window, judged by knowledge time. It fires on about 3% of issuer-days, so
the book is concentrated and turns over slowly.
"""
import datetime

from examples.huggingface.insider import mount_features
from examples.huggingface.hf_config import INSIDER10_UNIVERSE
from examples.huggingface.playbook import any_of, equities, hold, show_once

from ziplime.api import date_rules
from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"window": "insider10",
                 "description": "Hold names where three or more insiders bought within a fortnight"}

WINDOW = datetime.timedelta(days=30)
HOLD_FOR = datetime.timedelta(days=90)


async def initialize(context: TradingAlgorithm):
    context.universe = await equities(context, INSIDER10_UNIVERSE)
    context.source = await mount_features(context, fields=["is_cluster_buy"])
    context.opened_on = {}
    context.schedule_function(rebalance, date_rules.week_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    window = await data.history(assets=context.universe, since=WINDOW,
                                fields=["is_cluster_buy"], data_source=context.source)

    for sid in any_of(window, lambda r: r["is_cluster_buy"]):
        context.opened_on[sid] = today
    context.opened_on = {sid: opened for sid, opened in context.opened_on.items()
                         if today - opened < HOLD_FOR}

    held = await hold(context, data, context.opened_on)
    names = {a.sid: a.symbol for a in context.universe}
    show_once(context, f"first cluster signal, holding {len(held)}:",
              (names.get(sid, sid) for sid in sorted(held)))
