"""Hold all 125 names equally, read nothing. The control every other number is measured against.

Absolute returns from this universe are meaningless: it was selected from names whose insiders
traded actively in 2016 *and* which still have a price history in 2026, and 126 of the 251
candidates failed that second test. The ones that vanished are exactly those where insider buying
did not save the company.

This control carries the identical bias, so a rule that beats it has done something the survival
of these particular names does not already explain. A rule that loses to it has not.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from hf_config import INSIDER10_UNIVERSE  # noqa: E402
from insider import priced, rebalance  # noqa: E402

from ziplime.domain.bar_data import BarData  # noqa: E402
from ziplime.trading.trading_algorithm import TradingAlgorithm  # noqa: E402

STRATEGY_INFO = {"window": "insider10",
                 "description": "Equal-weight all 125 names, ignoring every filing -- the control"}
REBALANCE_EVERY_DAYS = 90


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
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
    await rebalance(context, data, {sid: weight for sid in live})
