"""A minimal MOEX FORTS futures algorithm.

Holds one Si contract, following the continuous future across rolls. The continuous future is a
data specifier, so what actually gets ordered is the contract it currently resolves to; when the
chain rolls, the old contract is closed and the new one opened.
"""
import structlog

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

logger = structlog.get_logger(__name__)

#: Contracts held in the front-month position.
CONTRACTS = 1


async def initialize(context: TradingAlgorithm):
    context.continuous_future = await context.continuous_future(
        "Si", offset=0, roll="volume", adjustment="mul")
    context.held_contract = None
    logger.info("Trading continuous future", continuous_future=str(context.continuous_future))


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.continuous_future)
    if contract is None:
        return

    if context.held_contract is not None and context.held_contract.sid != contract.sid:
        # The chain rolled: close the expiring contract before opening the new one.
        logger.info("Rolling", out=context.held_contract.symbol, into=contract.symbol)
        await context.order(asset=context.held_contract, amount=-CONTRACTS, style=MarketOrder())
        context.held_contract = None

    if context.held_contract is None:
        await context.order(asset=contract, amount=CONTRACTS, style=MarketOrder())
        context.held_contract = contract
