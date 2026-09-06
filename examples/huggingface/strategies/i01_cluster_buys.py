"""Buy where several insiders bought at once -- the dataset's own `is_cluster_buy`.

The canonical insider anomaly. One executive buys for a dozen reasons: a bonus to deploy, an
ownership guideline to meet, optimism about a house purchase. Three of them buying the same stock
on the open market inside a fortnight is much harder to explain by anything other than a shared
view of the company.

The flag is the publisher's: at least three distinct insiders making open-market purchases in a
trailing 14 calendar-day window, judged by knowledge time. It fires on 9 368 of 297 066 issuer-days
in 2022 -- roughly 3% -- so the book is concentrated and turns over slowly.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Hold names where three or more insiders bought within a fortnight"}
LOOKBACK = 30
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=["is_cluster_buy", "cluster_buyers_14d"])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=["is_cluster_buy"], data_source=context.source)
    firing = set()
    if not window.is_empty():
        firing = {r["sid"] for r in window.iter_rows(named=True) if r["is_cluster_buy"]}

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
