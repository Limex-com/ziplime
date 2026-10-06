"""Hold all 125 names equally, read nothing. The control every other number is measured against.

Absolute returns from this universe are meaningless: it was selected from names whose insiders
traded actively in 2016 *and* which still have a price history in 2026, and 126 of the 251
candidates failed that second test. The ones that vanished are exactly those where insider buying
did not save the company.

This control carries the identical bias, so a rule that beats it has done something the survival
of these particular names does not already explain. A rule that loses to it has not.
"""

from examples.huggingface.hf_config import INSIDER10_UNIVERSE
from examples.huggingface.portfolio import priced, rebalance_to

from ziplime.api import date_rules
from ziplime.domain.bar_data import BarData
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"window": "insider10",
                 "description": "Equal-weight all 125 names, ignoring every filing -- the control"}


async def initialize(context: TradingAlgorithm):
    context.universe = [await context.symbol(t, mic=m) for t, m in INSIDER10_UNIVERSE]
    context.schedule_function(rebalance, date_rules.quarter_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    live = await priced(context, data)
    if not live:
        return
    weight = 1.0 / len(live)
    await rebalance_to(context, data, {sid: weight for sid in live})
