"""Buy one 0DTE 525 call, which finishes out of the money and worthless."""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder

FIRST_SESSION = dt.date(2024, 6, 13)


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")


async def handle_data(context, data):
    if context.get_datetime().date() != FIRST_SESSION:
        return
    chain = await context.option_chain(context.underlying)
    await context.order(asset=chain.at_strike(525.0, OptionType.CALL), amount=1,
                        style=MarketOrder())
