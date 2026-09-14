"""compute_signals without a universe: the engine cannot guess which instruments to load."""


async def initialize(context):
    context.listing = await context.symbol("JNJ", mic="XNYS")


def compute_signals(context, prices):
    return {"long": prices.close > 0}


async def handle_data(context, data):
    pass
