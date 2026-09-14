"""The twin of ``signals_vectorised.py``, written the ordinary way.

Same averages, same thresholds, same orders -- recomputed from a fresh trailing window on every
bar. This is the version the vectorised one has to agree with, and the cost it has to beat: at
every one of the run's sessions it re-reads nineteen rows per instrument and re-averages them, for
a number one pass over the array already knows.

The window is built as ``history(SLOW - 1)`` plus ``current``, because ``data.history`` stops
before the current bar while the signal panel's row *is* the current bar -- the same convention
``data.current`` uses. Getting that off by one would not fail loudly; it would just quietly trade
a different strategy, which is why the equivalence test exists.
"""
import datetime as dt

from ziplime.finance.execution import MarketOrder

FAST, SLOW = 5, 20
SIZE = 100
CASH_FLOOR = 20_000.0


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


async def handle_data(context, data):
    for name, listing in context.universe.items():
        past = await data.history(assets=[listing], bar_count=SLOW - 1, fields=["close"],
                                  frequency=dt.timedelta(days=1))
        today = await data.current(assets=[listing], fields=["close"])
        closes = [*past["close"].to_list(), *today["close"].to_list()]
        if len(closes) < SLOW:
            continue

        fast = sum(closes[-FAST:]) / FAST
        slow = sum(closes[-SLOW:]) / SLOW
        wanted = fast > slow
        held = await context.portfolio.get_asset_positions_amount(listing)

        if wanted and held == 0 and context.portfolio.cash > CASH_FLOOR:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif not wanted and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
