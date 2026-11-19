"""The syntax from the design discussion: compare two strategies, buy units of one."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.strategy1 = await context.strategy("s1")
    context.strategy2 = await context.strategy("s2")
    context.seen = []


async def handle_data(context, data):
    cash1 = context.strategy1.portfolio.cash
    sharpe1 = context.strategy1.performance.sharpe
    sharpe2 = context.strategy2.performance.sharpe
    context.seen.append((cash1, sharpe1, sharpe2))
    if sharpe1 > sharpe2 and context.strategy1.units == 0:
        # Through the strategy's portfolio, as first sketched; the handle itself works the same.
        await context.order(context.strategy1.portfolio, 50_000, style=MarketOrder())
