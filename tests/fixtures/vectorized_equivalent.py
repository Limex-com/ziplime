"""Places exactly the trades the vectorised test drives vectorbt with.

Both engines have to see the same decisions before their outputs can be compared, so the dates
and sizes live here as literals rather than being derived twice.
"""
import datetime as dt

from ziplime.finance.execution import MarketOrder

#: ``session -> {ticker: signed contracts}``. Mirrored in tests/test_vectorized.py.
SCHEDULE = {
    dt.date(2024, 1, 10): {"JNJ": 100},
    dt.date(2024, 1, 24): {"KO": 200},
    dt.date(2024, 2, 14): {"JNJ": -100},
    dt.date(2024, 3, 6): {"KO": -200},
}


async def initialize(context):
    context.listings = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


async def handle_data(context, data):
    orders = SCHEDULE.get(context.get_datetime().date())
    if not orders:
        return
    for ticker, amount in orders.items():
        await context.order(asset=context.listings[ticker], amount=amount, style=MarketOrder())
