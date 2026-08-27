"""Roll on the calendar instead of on volume.

``roll="calendar"`` moves to the next contract a fixed distance before auto close, regardless of
where liquidity is. It is predictable, which matters when you want the roll date known in advance;
``roll="volume"`` instead follows where the trading actually went.

The distance is a property of the data bundle rather than of the algorithm, so it is set through
``roll_finder_settings`` -- here, ten days ahead of auto close, which rolls out of the expiring
contract well before its final sessions thin out.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["Si"],
    "description": "Calendar roll, ten days ahead of auto close",
    "roll_finder_settings": {"calendar": {"roll_offset_days": 10}},
}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="calendar",
                                                    adjustment="mul")
    context.held = None
    context.rolls = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
        context.rolls += 1
    if context.held is None:
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held = contract
