"""A moving-average cross with the arithmetic vectorised and the decisions left bar by bar.

The pair of this file and ``signals_bar_by_bar.py`` is the point of the whole mechanism: the two
express the same strategy, one with ``compute_signals`` and one with ``data.history`` on every
bar, and the test asserts they produce the same performance table. If they ever stop agreeing,
the vectorised path is computing something the bar-by-bar path is not, and the speed is worthless.

Note what stays here rather than moving into the array pass: the cash floor and the "already
holding it" check both read state that only exists once the run has reached this bar. That split
is the rule -- signals vectorise, decisions do not.
"""

from ziplime.finance.execution import MarketOrder

FAST, SLOW = 5, 20
SIZE = 100
#: Do not open a new position below this much cash. Depends on every fill before it, so it cannot
#: be computed in advance at any price.
CASH_FLOOR = 20_000.0

#: Sessions of history loaded before the run's first bar, for the slow average to warm up on.
WARMUP = SLOW


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


def compute_signals(context, prices):
    """One pass over the whole history, before the first bar.

    Every operation here is a trailing window or element-wise, which is what makes the result the
    same as computing it bar by bar -- and what lets :func:`verify_causality` prove it.

    The averages are returned rather than the ``fast > slow`` comparison on purpose. A comparison
    turns the warm-up NaN into a plain ``False``, so a boolean signal cannot say whether it is
    warmed up or merely negative, and ``is_ready`` has nothing to read. Return the numbers and
    compare them at the bar.
    """
    return {
        "fast": prices.close.rolling(FAST).mean(),
        "slow": prices.close.rolling(SLOW).mean(),
    }


async def handle_data(context, data):
    if not (context.signals.is_ready("fast") and context.signals.is_ready("slow")):
        return

    for name, listing in context.universe.items():
        wanted = float(context.signals["fast"][name]) > float(context.signals["slow"][name])
        held = await context.portfolio.get_asset_positions_amount(listing)

        if wanted and held == 0 and context.portfolio.cash > CASH_FLOOR:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif not wanted and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
