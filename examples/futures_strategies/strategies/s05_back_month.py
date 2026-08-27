"""Trade the second contract out instead of the front month.

``offset=1`` walks one step down the chain. Back months are less liquid but roll less often, and
for a carry trade they are where the shape of the curve is.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Long the second contract out (offset=1)"}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=1, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held = contract
