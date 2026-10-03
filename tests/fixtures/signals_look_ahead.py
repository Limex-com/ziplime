"""A strategy that reads tomorrow's close, and must never be allowed to report a return.

Written the way the mistake actually happens: `shift(-1)` looks like a neighbour of `shift(1)`,
and the resulting equity curve is beautiful rather than obviously broken.
"""
from ziplime.finance.execution import MarketOrder

WARMUP = 5


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ", mic="XNYS")}


def compute_signals(context, prices):
    return {"up_tomorrow": prices.close.shift(-1) > prices.close}


async def handle_data(context, data):
    if context.signals["up_tomorrow"]["JNJ"]:
        await context.order(asset=context.universe["JNJ"], amount=100, style=MarketOrder())
