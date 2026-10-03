"""Buy a first-session contract, then try to trade it again after it has expired."""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder

FIRST_SESSION = dt.date(2024, 6, 13)


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")
    context.expired = None


async def handle_data(context, data):
    session = context.get_datetime().date()
    if session == FIRST_SESSION:
        chain = await context.option_chain(context.underlying)
        context.expired = chain.at_strike(520.0, OptionType.CALL)
        await context.order(asset=context.expired, amount=1, style=MarketOrder())
    elif context.expired is not None:
        # Yesterday's contract. It no longer trades, and the engine must refuse it.
        await context.order(asset=context.expired, amount=1, style=MarketOrder())
