"""A fast SMA cross on minute bars, for the intraday-metrics tests: it has to actually trade."""
import datetime as dt

from ziplime.finance.execution import MarketOrder

FAST, SLOW = 5, 20


async def initialize(context):
    context.listing = await context.symbol("AAPL", mic="XNGS")


async def handle_data(context, data):
    past = await data.history(assets=[context.listing], bar_count=SLOW - 1, fields=["close"],
                              frequency=dt.timedelta(minutes=1))
    today = await data.current(assets=[context.listing], fields=["close"])
    closes = [*past["close"].to_list(), *today["close"].to_list()]
    if len(closes) < SLOW:
        return

    wanted = (sum(closes[-FAST:]) / FAST) > (sum(closes[-SLOW:]) / SLOW)
    held = await context.portfolio.get_asset_positions_amount(context.listing)
    if wanted and held == 0:
        await context.order(asset=context.listing, amount=200, style=MarketOrder())
    elif not wanted and held > 0:
        await context.order(asset=context.listing, amount=-held, style=MarketOrder())
