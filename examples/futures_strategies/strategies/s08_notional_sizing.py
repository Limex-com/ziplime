"""Size a position by notional exposure, not by price.

One contract of Si is worth price x multiplier roubles, so ``cash / price`` is the wrong number of
contracts. ``context.contracts_for_notional`` does the multiplier arithmetic and rounds toward
zero, because a futures position is a whole number of contracts.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Target a fixed notional exposure per contract"}
TARGET_NOTIONAL = 400_000.0


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.held_amount = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    current = await data.current(assets=[contract], fields=["close"])
    if len(current) == 0 or current["close"][0] is None:
        return
    price = current["close"][0]

    wanted = context.contracts_for_notional(asset=contract, notional=TARGET_NOTIONAL, price=price)
    rolled = context.held is not None and context.held.sid != contract.sid
    if context.held is not None and (rolled or wanted != context.held_amount):
        await context.order(asset=context.held, amount=-context.held_amount, style=MarketOrder())
        context.held, context.held_amount = None, 0
    if context.held is None and wanted:
        await context.order(asset=contract, amount=wanted, style=MarketOrder())
        context.held, context.held_amount = contract, wanted
