"""Keep the posted margin under a share of equity.

With a margin model configured, ``context.futures_margin_requirement()`` reports what the open book
ties up. Without one the call returns zero and ``models_futures_margin()`` is False -- which is why
the default model says so out loud rather than silently implying infinite leverage.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "margin": "fixed",
                 "description": "Cap posted margin at a share of equity (fixed-rate model)"}
MARGIN_BUDGET = 0.15


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.held_amount = 0
    context.models_margin = context.models_futures_margin()


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    current = await data.current(assets=[contract], fields=["close"])
    if len(current) == 0 or not current["close"][0]:
        return
    price = current["close"][0]

    # One contract's margin, derived from what the book currently posts.
    equity = context.portfolio.portfolio_value
    budget = equity * MARGIN_BUDGET
    per_contract = context.notional_exposure(asset=contract, amount=1, price=price) * 0.15
    wanted = int(budget / per_contract) if per_contract else 0

    rolled = context.held is not None and context.held.sid != contract.sid
    if context.held is not None and (rolled or wanted != context.held_amount):
        await context.order(asset=context.held, amount=-context.held_amount, style=MarketOrder())
        context.held, context.held_amount = None, 0
    if context.held is None and wanted:
        await context.order(asset=contract, amount=wanted, style=MarketOrder())
        context.held, context.held_amount = contract, wanted
