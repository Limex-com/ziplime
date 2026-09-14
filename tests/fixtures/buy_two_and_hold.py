"""Buy both names on the first session and hold. One of them stops trading partway through."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.holdings = [await context.symbol("JNJ", mic="XNYS"),
                        await context.symbol("KO", mic="XNYS")]
    context.bought = False


async def handle_data(context, data):
    if context.bought:
        return
    for asset in context.holdings:
        await context.order_target(asset=asset, target=10, style=MarketOrder())
    context.bought = True
