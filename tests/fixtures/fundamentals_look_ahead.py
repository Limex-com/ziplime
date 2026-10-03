"""Reads the next filing before it is filed -- has to be refused before the first order."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ", mic="XNYS")}
    context.datasets = {"fundamentals": "fundamentals"}


def compute_signals(context, prices):
    f = prices.dataset("fundamentals")
    return {"next_revenue": f.revenue.shift(-1)}


async def handle_data(context, data):
    if context.signals.is_ready("next_revenue"):
        await context.order(asset=context.universe["JNJ"], amount=10, style=MarketOrder())
