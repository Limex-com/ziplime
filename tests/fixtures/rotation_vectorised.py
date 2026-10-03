"""Hold whichever name has the strongest trailing momentum, with the ranking vectorised.

A rotation is the shape a benchmark most wants and the fixtures did not have: its signal is
*cross-sectional*, so every bar has to look across the universe rather than down one series. Bar
by bar that means one history window per name per bar; in one pass it is a single ``rank`` over a
frame. The gap between the two is the whole case for the vectorised path, and it widens with the
size of the universe rather than staying flat the way a per-name average does.

Its twin ``rotation_bar_by_bar.py`` computes the identical ranking from ``data.history``. They
must place the same orders -- ``tests/test_engine_speed.py`` asserts it -- or the speed is being
bought with a different strategy.
"""
from ziplime.finance.execution import MarketOrder

LOOKBACK = 20
SIZE = 50
#: Sessions of history before the first bar, for the momentum window to warm up on.
WARMUP = LOOKBACK


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


def compute_signals(context, prices):
    """Trailing return per name, ranked across each row.

    Ranked along ``axis=1`` -- across the universe within one bar -- which is what keeps it
    causal. A rank computed down the column would be a rank against the future, and
    ``verify_causality`` refuses the run for it.
    """
    momentum = prices.close.pct_change(LOOKBACK)
    return {"rank": momentum.rank(axis=1, ascending=False), "momentum": momentum}


async def handle_data(context, data):
    if not context.signals.is_ready("rank"):
        return

    for name, listing in context.universe.items():
        leader = float(context.signals["rank"][name]) == 1.0
        held = await context.portfolio.get_asset_positions_amount(listing)
        if leader and held == 0:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif not leader and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
