"""Buy where an insider raised their own stake by a large percentage.

Dollar size says how rich the insider is; percentage says how convinced they are. An executive who
already owns ten million dollars of stock and buys another hundred thousand has changed nothing
about their exposure. One who raises their holding by half has.

`max_holding_change_pct` is the dataset's own before-and-after computation, and it is thin: the
publisher emits it only where the position logic is unambiguous, which is 26% of transaction rows
and about 3% of issuer-days. This is the smallest sample in the suite, and its result should be
read with that in mind.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import mount_features, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10", "description": "Hold names where an insider raised their own position by a large share"}
LOOKBACK = 30
HOLD_DAYS = 90
REBALANCE_EVERY_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.source = await mount_features(context, fields=['max_holding_change_pct'])
    context.opened_on = {}
    context.held = frozenset()
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=['max_holding_change_pct'], data_source=context.source)
    firing = set()
    if not window.is_empty():
        firing = {r["sid"] for r in window.iter_rows(named=True)
                  if (r["max_holding_change_pct"] or 0.0) >= 0.25}

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
