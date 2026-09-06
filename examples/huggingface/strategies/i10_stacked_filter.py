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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Require a buying cluster and an officer among the buyers"}
LOOKBACK = 30
HOLD_DAYS = 120
REBALANCE_EVERY_DAYS = 7
FIELDS = ["is_cluster_buy", "n_ceo_buys", "n_cfo_buys"]


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=FIELDS)
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=FIELDS, data_source=context.source)
    clustered, officer = set(), set()
    if not window.is_empty():
        for row in window.iter_rows(named=True):
            if row["is_cluster_buy"]:
                clustered.add(row["sid"])
            if (row["n_ceo_buys"] or 0) + (row["n_cfo_buys"] or 0) > 0:
                officer.add(row["sid"])

    for sid in clustered & officer:
        context.opened_on[sid] = today
    context.opened_on = {s: d for s, d in context.opened_on.items()
                         if (today - d).days < HOLD_DAYS}
    held = frozenset(context.opened_on)
    if held == context.held:
        return
    context.held = held
    weight = 1.0 / len(held) if held else 0.0
    await rebalance(context, data, {sid: weight for sid in held})
