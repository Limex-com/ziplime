"""Records what it sees of its strategy on each bar: the strategy has always had exactly as many."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.held = await context.strategy("hold", config={"symbol": "JNJ", "weight": 1.0})
    context.bars = 0
    context.lag = []


async def handle_data(context, data):
    context.bars += 1
    context.lag.append(context.held.context.bars - context.bars)
    await context.order_target_percent(context.held, 1.0, style=MarketOrder())
