"""Hold all 230 names equally and read no filing. The control.

Same universe, same survivorship, no fundamentals. Every number in the suite is only meaningful
against this one: these companies were selected for reporting completely *and* still having a price
in 2026, and both conditions favour the survivors.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import FUNDAMENTALS_UNIVERSE  # noqa: E402
from insider import priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "fundamentals",
                 "description": "Equal-weight all 230 names, reading no filing -- the control"}
REBALANCE_EVERY_DAYS = 90


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in FUNDAMENTALS_UNIVERSE]
    context.last_rebalance = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    live = await priced(context, data)
    if not live:
        return
    context.last_rebalance = today
    weight = 1.0 / len(live)
    # A quarter of the target, not a flat 1%. With 230 names each target is 0.43%, so a 1% band is
    # wider than the position itself and nothing ever trades -- which is exactly what an earlier
    # version of this did, reporting a return of 0.00% on zero trades.
    await rebalance(context, data, {sid: weight for sid in live}, tolerance=weight * 0.25)
