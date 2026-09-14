"""The same future, computed from data.history on every bar."""
import datetime as dt

from ziplime.assets.domain.asset_type import AssetType
from ziplime.finance.execution import MarketOrder

FAST, SLOW = 3, 10
SIZE = 2


async def initialize(context):
    context.universe = {"CL": await context.symbol("CLZ23", mic="XCME",
                                                   asset_type=AssetType.FUTURES_CONTRACT)}


async def handle_data(context, data):
    listing = context.universe["CL"]
    past = await data.history(assets=[listing], bar_count=SLOW - 1, fields=["close"],
                              frequency=dt.timedelta(days=1))
    today = await data.current(assets=[listing], fields=["close"])
    closes = [*past["close"].to_list(), *today["close"].to_list()]
    if len(closes) < SLOW:
        return

    wanted = (sum(closes[-FAST:]) / FAST) > (sum(closes[-SLOW:]) / SLOW)
    held = await context.portfolio.get_asset_positions_amount(listing)
    if wanted and held == 0:
        await context.order(asset=listing, amount=SIZE, style=MarketOrder())
    elif not wanted and held != 0:
        await context.order(asset=listing, amount=-held, style=MarketOrder())
