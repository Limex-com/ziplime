"""Hold all 230 names equally and read no filing. The control.

Same universe, same survivorship, no fundamentals. Every number in the suite is only meaningful
against this one: these companies were selected for reporting completely *and* still having a price
in 2026, and both conditions favour the survivors.
"""

from examples.huggingface.hf_config import FUNDAMENTALS_UNIVERSE
from examples.huggingface.portfolio import priced, rebalance_to

from ziplime.api import date_rules
from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"window": "fundamentals",
                 "description": "Equal-weight all 230 names, reading no filing -- the control"}


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in FUNDAMENTALS_UNIVERSE]
    context.schedule_function(rebalance, date_rules.quarter_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    live = await priced(context, data)
    if not live:
        return
    weight = 1.0 / len(live)
    # A quarter of the target, not a flat 1%. With 230 names each target is 0.43%, so a 1% band is
    # wider than the position itself and nothing ever trades -- which is exactly what an earlier
    # version of this did, reporting a return of 0.00% on zero trades.
    await rebalance_to(context, data, {sid: weight for sid in live}, tolerance=weight * 0.25)
