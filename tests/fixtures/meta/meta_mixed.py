"""Strategies and listings in one rebalance; a commission on strategy units; a broken strategy."""
from ziplime.finance.commission import PerDollar
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.s1 = await context.strategy("s1", commission=PerDollar(cost=0.01))
    context.broken = await context.strategy("broken")
    context.ko = await context.symbol("KO", mic="XNYS")


async def handle_data(context, data):
    await context.rebalance({context.s1: 0.5, context.ko: 0.4}, style=MarketOrder())
