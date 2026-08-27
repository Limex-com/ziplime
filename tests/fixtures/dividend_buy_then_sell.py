import datetime as dt

from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.asset = await context.symbol("SBER", mic="MISX")


async def handle_data(context, data):
    session = context.get_datetime().date()
    if session == dt.date(2024, 7, 8):
        await context.order_target(
            asset=context.asset,
            target=10,
            style=MarketOrder(),
        )
    elif session == dt.date(2024, 7, 12):
        await context.order_target(
            asset=context.asset,
            target=0,
            style=MarketOrder(),
        )
