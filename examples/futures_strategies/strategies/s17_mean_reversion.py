"""Fade moves away from a rolling mean, measured in standard deviations.

A z-score rule needs a continuous price history to be meaningful -- computed on unadjusted prices,
every roll would inject a fake deviation and the rule would trade the contract gap.
"""
import statistics

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Z-score mean reversion on the adjusted chain"}
WINDOW = 30
ENTRY_Z, EXIT_Z = 1.5, 0.3


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.direction = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    history = await data.history(assets=[context.chain], bar_count=WINDOW, fields=["close"],
                                 frequency="1d")
    closes = history["close"].to_list() if len(history) else []
    if len(closes) < WINDOW or any(not c for c in closes):
        return

    mean = statistics.fmean(closes)
    spread = statistics.pstdev(closes)
    if spread <= 0:
        return
    z = (closes[-1] - mean) / spread

    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-context.direction, style=MarketOrder())
        context.held, context.direction = None, 0

    if context.held is not None and abs(z) <= EXIT_Z:
        await context.order(asset=context.held, amount=-context.direction, style=MarketOrder())
        context.held, context.direction = None, 0
    elif context.held is None and abs(z) >= ENTRY_Z:
        direction = -1 if z > 0 else 1          # fade the move
        await context.order(asset=contract, amount=direction, style=MarketOrder())
        context.held, context.direction = contract, direction
