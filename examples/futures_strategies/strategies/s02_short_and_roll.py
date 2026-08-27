"""The same position held short, to show that futures P&L is symmetric.

Nothing about shorting a future is special: there is no borrow, and the margin requirement is the
same as the long side.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Short front month, rolled with the chain"}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=1, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=contract, amount=-1, style=MarketOrder())
        context.held = contract
