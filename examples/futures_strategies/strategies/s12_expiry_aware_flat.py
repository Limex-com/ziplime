"""Go flat a few sessions before expiry instead of rolling.

Liquidity thins and the spread widens into the last sessions of a contract. This rule reads the
contract's own ``expiration_date`` and stands aside rather than trading through it.
"""
import datetime

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Flat in the last sessions before expiry"}
FLAT_WITHIN_DAYS = 7


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    today = context.get_datetime().date()
    days_left = (contract.asset.expiration_date - today).days
    near_expiry = days_left <= FLAT_WITHIN_DAYS

    if context.held is not None and (context.held.sid != contract.sid or near_expiry):
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
    if context.held is None and not near_expiry:
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held = contract
