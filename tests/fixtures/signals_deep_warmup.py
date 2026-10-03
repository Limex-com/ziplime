"""Asks for more warm-up than the bundle holds, which the run has to say out loud."""
WARMUP = 5_000


async def initialize(context):
    context.universe = {"JNJ": await context.symbol("JNJ", mic="XNYS")}


def compute_signals(context, prices):
    return {"slow": prices.close.rolling(200).mean()}


async def handle_data(context, data):
    pass
