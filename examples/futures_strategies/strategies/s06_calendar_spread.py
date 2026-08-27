"""Long the front month against the second month: a calendar spread.

Two chains at different offsets, held in opposite directions. The position is close to flat in
outright price and exposed instead to the shape of the curve, so its P&L is much smaller than
either leg -- which is the point.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Calendar spread: long front, short second month"}


async def initialize(context: TradingAlgorithm):
    context.front = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.back = await context.continuous_future("Si", offset=1, roll="volume", adjustment="mul")
    context.legs = {}


async def _hold(context, data, chain, key, amount):
    contract = await data.current_contract(chain)
    if contract is None:
        return
    held = context.legs.get(key)
    if held is not None and held.sid != contract.sid:
        await context.order(asset=held, amount=-amount, style=MarketOrder())
        context.legs[key] = None
        held = None
    if held is None:
        await context.order(asset=contract, amount=amount, style=MarketOrder())
        context.legs[key] = contract


async def handle_data(context: TradingAlgorithm, data: BarData):
    await _hold(context, data, context.front, "front", 1)
    await _hold(context, data, context.back, "back", -1)
