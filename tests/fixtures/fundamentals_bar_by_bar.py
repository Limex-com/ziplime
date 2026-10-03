"""The same strategy reading the statements one bar at a time, for comparison."""

from ziplime.finance.execution import MarketOrder

SIZE = 100
FIELDS = ["revenue", "gross_profit", "total_assets"]


async def initialize(context):
    context.universe = {
        "JNJ": await context.symbol("JNJ", mic="XNYS"),
        "KO": await context.symbol("KO", mic="XNYS"),
    }


async def handle_data(context, data):
    statements = await data.current(assets=list(context.universe.values()), fields=FIELDS,
                                    data_source="fundamentals")
    if statements.is_empty():
        return
    by_sid = {row["sid"]: row for row in statements.iter_rows(named=True)}

    scores = {}
    for name, listing in context.universe.items():
        row = by_sid.get(listing.sid)
        if row is None or row.get("gross_profit") is None or row.get("total_assets") is None:
            return          # the vectorised twin sits out the bar too: its signal is NaN
        scores[name] = row["gross_profit"] / row["total_assets"]
    if len(scores) < len(context.universe):
        return

    best = max(scores, key=lambda name: scores[name])
    for name, listing in context.universe.items():
        held = await context.portfolio.get_asset_positions_amount(listing)
        if name == best and held == 0:
            await context.order(asset=listing, amount=SIZE, style=MarketOrder())
        elif name != best and held > 0:
            await context.order(asset=listing, amount=-held, style=MarketOrder())
