"""Size against margin quoted per contract, the way an exchange publishes it.

``PerRootFuturesMarginModel`` takes a rouble amount per contract per root, which is exactly what
MOEX's ``GetAssetParams`` returns. The strategy reads back the requirement it has posted and stops
adding when the budget is used up.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si", "SR"], "margin": "per_root",
                 "description": "Per-contract margin budget across two roots"}
MARGIN_BUDGET = 0.30


async def initialize(context: TradingAlgorithm):
    context.chains = [await context.continuous_future(root, offset=0, roll="volume",
                                                      adjustment="mul")
                      for root in ("Si", "SR")]
    context.held = {}


async def handle_data(context: TradingAlgorithm, data: BarData):
    budget = context.portfolio.portfolio_value * MARGIN_BUDGET
    for chain in context.chains:
        contract = await data.current_contract(chain)
        if contract is None:
            continue
        held = context.held.get(chain.root_symbol)
        if held is not None and held.sid != contract.sid:
            await context.order(asset=held, amount=-1, style=MarketOrder())
            context.held[chain.root_symbol] = None
            held = None
        # Only add while the posted margin is still inside the budget.
        if held is None and context.futures_margin_requirement() < budget:
            await context.order(asset=contract, amount=1, style=MarketOrder())
            context.held[chain.root_symbol] = contract
