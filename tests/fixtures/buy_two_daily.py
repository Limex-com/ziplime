"""Order both names every session, so a name that stops trading is asked for after it stops."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.holdings = [await context.symbol("JNJ", mic="XNYS"),
                        await context.symbol("KO", mic="XNYS")]


async def handle_data(context, data):
    for asset in context.holdings:
        await context.order(asset=asset, amount=1, style=MarketOrder())
