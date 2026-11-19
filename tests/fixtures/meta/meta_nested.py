"""A metastrategy of metastrategies: it holds meta_lockstep, which holds hold."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.inner = await context.strategy("meta_lockstep")


async def handle_data(context, data):
    await context.order_target_percent(context.inner, 1.0, style=MarketOrder())
