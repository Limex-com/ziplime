"""A moving-average cross with nothing in it that a signal array cannot express.

The twin of the vector side in `tests/test_vector_kernel_parity.py`, and deliberately plainer than
`signals_bar_by_bar.py`: no cash floor, no exposure cap, no sizing off portfolio value. Every one
of those is a decision that reads the book, and a decision that reads the book is precisely what a
boolean entries/exits pair cannot carry -- so including one would make the two sides different
strategies and the parity test would be measuring that difference instead of the engines.

What is left maps one-to-one:

    fast > slow  and flat   ->  entries[t, name]
    fast < slow  and long   ->  exits[t, name]

The window is `history(SLOW - 1)` plus `current`, which spans SLOW closes ending at this bar --
the same span as `rolling(SLOW).mean()` on the vector side. Off by one here would not fail
loudly; it would quietly trade a different system a bar late.
"""
import datetime as dt

from ziplime.finance.execution import MarketOrder

FAST, SLOW = 5, 20
SIZE = 100


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
        held = await context.portfolio.get_asset_positions_amount(listing)

        if fast > slow and held == 0:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif fast < slow and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
