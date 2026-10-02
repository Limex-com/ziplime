"""Swap the whole book between two names every session -- the shape a daily rebalance has."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.pair = [await context.symbol("JNJ", mic="XNYS"),
                    await context.symbol("KO", mic="XNYS")]
    context.bar = 0


async def handle_data(context, data):
    hold, drop = context.pair if context.bar % 2 == 0 else context.pair[::-1]
    await context.order_target_percent(asset=drop, target=0.0, style=MarketOrder())
    await context.order_target_percent(asset=hold, target=0.9, style=MarketOrder())
    context.bar += 1
