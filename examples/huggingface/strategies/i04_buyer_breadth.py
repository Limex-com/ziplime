"""Buy where many *different* insiders have been buying, using the 30-day unique-buyer count.

Breadth, not clustering. `is_cluster_buy` demands three distinct buyers inside a 14-day window --
a tight, rare event. This asks a looser question over a longer horizon: how many separate people
at this company have bought in the last month, whether or not they did it at the same time.

If the cluster flag is capturing something real, a looser version of the same idea should capture
a weaker version of it. If `i01` beats this and `i00`, the tightness is doing the work.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features
from portfolio import rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names with several distinct insider buyers over the past 30 days"}
LOOKBACK = 30
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=['n_unique_buyers_30d'])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=['n_unique_buyers_30d'], data_source=context.source)
    firing = set()
    if not window.is_empty():
        firing = {r["sid"] for r in window.iter_rows(named=True)
                  if (r["n_unique_buyers_30d"] or 0) >= 3}

    for sid in firing:
        context.opened_on[sid] = today
    context.opened_on = {s: d for s, d in context.opened_on.items()
                         if (today - d).days < HOLD_DAYS}
    held = frozenset(context.opened_on)
    if held == context.held:
        return
    context.held = held
    weight = 1.0 / len(held) if held else 0.0
    await rebalance(context, data, {sid: weight for sid in held})
