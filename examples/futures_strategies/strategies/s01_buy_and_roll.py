"""Hold one long contract and follow the chain across every roll.

The baseline futures pattern. A continuous future is a *data specifier*, not something you can
order -- so the algorithm asks it which contract is live today and trades that.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Long front month, rolled with the chain"}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    if context.held is not None and context.held.sid != contract.sid:
        # A roll is two real trades: close the expiring contract, open the new one.
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held = contract
