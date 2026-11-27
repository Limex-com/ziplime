"""A cluster of `hold`: two names x two weights. Rotates into whichever leads on total return."""
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.cluster = await context.strategy(
        "hold", grid={"symbol": ["JNJ", "KO"], "weight": {"from": 0.5, "to": 1.0, "step": 0.5}})
    context.picks = []


async def handle_data(context, data):
    best = context.cluster.best("returns")
    context.picks.append(best.label)
    await context.rebalance({best: 0.95}, style=MarketOrder())
