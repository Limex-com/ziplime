from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.asset = await context.symbol("JNJ", mic="XNYS")


async def handle_data(context, data):
    await context.order_target_percent(context.asset, 0.9, style=MarketOrder())
