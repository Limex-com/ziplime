"""The twin of ``rotation_vectorised.py``, ranking the universe again on every bar.

The cost this version pays is the one the vectorised twin removes: a history window per name per
bar, and a ranking rebuilt from them, for a number a single pass already knows for every bar.

``history(LOOKBACK)`` plus ``current`` spans ``LOOKBACK + 1`` closes, because ``data.history``
stops before the current bar while ``pct_change(LOOKBACK)`` measures from the close ``LOOKBACK``
bars back to this one. Off by one here does not raise -- it silently trades a different strategy.
"""
import datetime as dt

from ziplime.finance.execution import MarketOrder

LOOKBACK = 20
SIZE = 50


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


async def handle_data(context, data):
    momentum = {}
    for name, listing in context.universe.items():
        past = await data.history(assets=[listing], bar_count=LOOKBACK, fields=["close"],
                                  frequency=dt.timedelta(days=1))
        today = await data.current(assets=[listing], fields=["close"])
        closes = [*past["close"].to_list(), *today["close"].to_list()]
        if len(closes) < LOOKBACK + 1:
            return
        momentum[name] = closes[-1] / closes[-(LOOKBACK + 1)] - 1.0

    best = max(momentum, key=lambda name: momentum[name])
    for name, listing in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(listing)
        if name == best and held == 0:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif name != best and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
