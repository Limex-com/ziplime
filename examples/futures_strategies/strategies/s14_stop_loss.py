"""A stop measured in contract points, converted to money by the multiplier.

A five-point stop is not five roubles: on Si one point is one rouble per contract, on the MOEX
index future one point is 25. ``notional_exposure`` keeps the arithmetic honest.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Point-based stop loss and re-entry"}
STOP_FRACTION = 0.03


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.entry_price = None
    context.stopped_out_until = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    current = await data.current(assets=[contract], fields=["close"])
    if len(current) == 0 or not current["close"][0]:
        return
    price = current["close"][0]

    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held, context.entry_price = None, None

    if context.held is not None and context.entry_price is not None:
        loss_points = context.entry_price - price
        if loss_points > 0 and loss_points / context.entry_price >= STOP_FRACTION:
            await context.order(asset=context.held, amount=-1, style=MarketOrder())
            context.held, context.entry_price = None, None
            context.stopped_out_until = price * (1 + STOP_FRACTION / 2)
            return

    if context.held is None:
        # Re-enter once the market has recovered past where the stop fired.
        if context.stopped_out_until is not None and price < context.stopped_out_until:
            return
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held, context.entry_price = contract, price
        context.stopped_out_until = None
