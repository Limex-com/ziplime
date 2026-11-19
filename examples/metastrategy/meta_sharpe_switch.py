"""Two strategies, one allocation: hold whichever has the better Sharpe over the last quarter.

The strategies trade on their own, as if run alone; this algorithm buys and sells *units* of them at
their NAV. A strategy is ordered like an asset -- order, order_target_percent, rebalance.
"""
from ziplime.finance.execution import MarketOrder

LOOKBACK = 63  # sessions


async def initialize(context):
    context.strategy1 = await context.strategy("bollinger")              # strategies/bollinger.py
    context.strategy2 = await context.strategy("trend", config={"fast": 20, "slow": 100})


async def handle_data(context, data):
    s1 = context.strategy1.performance.last(LOOKBACK)
    s2 = context.strategy2.performance.last(LOOKBACK)
    if s1.days < LOOKBACK:
        return                                                         # not enough record yet
    leader = context.strategy1 if s1.sharpe > s2.sharpe else context.strategy2
    if leader.weight < 0.9:                                            # switch only when it changes
        await context.rebalance({leader: 0.95}, style=MarketOrder())   # the other one is closed

    context.record(nav1=context.strategy1.nav, nav2=context.strategy2.nav,
                   leader=1 if leader is context.strategy1 else 2)
