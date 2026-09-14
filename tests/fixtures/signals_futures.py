"""Vectorised signals on a future: the multiplier and the margin stay entirely with ziplime."""
from ziplime.assets.domain.asset_type import AssetType
from ziplime.finance.execution import MarketOrder

FAST, SLOW = 3, 10
WARMUP = SLOW
SIZE = 2


async def initialize(context):
    context.universe = {"CL": await context.symbol("CLZ23", mic="XCME",
                                                   asset_type=AssetType.FUTURES_CONTRACT)}


def compute_signals(context, prices):
    return {"fast": prices.close.rolling(FAST).mean(),
            "slow": prices.close.rolling(SLOW).mean()}


async def handle_data(context, data):
    if not (context.signals.is_ready("fast") and context.signals.is_ready("slow")):
        return
    listing = context.universe["CL"]
    wanted = float(context.signals["fast"]["CL"]) > float(context.signals["slow"]["CL"])
    held = await context.portfolio.get_asset_positions_amount(listing)
    if wanted and held == 0:
        await context.order(asset=listing, amount=SIZE, style=MarketOrder())
    elif not wanted and held != 0:
        await context.order(asset=listing, amount=-held, style=MarketOrder())
