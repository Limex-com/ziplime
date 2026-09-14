"""Rank on gross profitability, with the ranking computed over the whole history in one pass.

The only line that is new is `prices.dataset("fundamentals")`: it returns the statements as of
every bar, on the same index as the prices, so a fundamental reads exactly like a price.
"""
from ziplime.finance.execution import MarketOrder

WARMUP = 0
SIZE = 100


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }
    # Name -> mounted source. The engine resolves each as of every bar.
    context.datasets = {"fundamentals": "fundamentals"}


def compute_signals(context, prices):
    f = prices.dataset("fundamentals")
    return {"quality": f.gross_profit / f.total_assets}


async def handle_data(context, data):
    scores = {name: float(context.signals["quality"][name]) for name in context.universe}
    # Sit out until every name has a statement. `is_ready` is not enough here: it asks whether the
    # row is entirely warm-up, and one company can have filed while the other has not -- which
    # would put a NaN into the comparison below, where it loses silently rather than raising.
    if any(score != score for score in scores.values()):
        return

    best = max(scores, key=lambda name: scores[name])

    for name, listing in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(listing)
        if name == best and held == 0:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif name != best and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
