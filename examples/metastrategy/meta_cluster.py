"""A parameter cluster run side by side; capital goes to the best few by recent Sharpe.

Every combination of the grid is its own strategy with its own book -- 3 x 3 = 9 of them here.
Once a month the three with the best Sharpe over the last 60 sessions get a third each.
"""
from ziplime.finance.execution import MarketOrder
from ziplime.utils.events import date_rules, time_rules


async def initialize(context):
    context.bb = await context.strategy(
        "bollinger",
        grid={"bb_period": [10, 20, 30], "bb_std": {"from": 1.5, "to": 2.5, "step": 0.5}},
    )
    context.schedule_function(reallocate, date_rules.month_start(), time_rules.market_close())


async def reallocate(context, data):
    ready = [member for member in context.bb if member.performance.days >= 60]
    if not ready:
        return
    best = context.bb.top(3, by="sharpe", window=60)
    await context.rebalance({member: 0.95 / len(best) for member in best}, style=MarketOrder())


def analyze(context, perf):
    # Every combination's record, ranked: parameters and metrics in one table.
    print(context.bb.to_frame().sort_values("sharpe", ascending=False).round(3))
