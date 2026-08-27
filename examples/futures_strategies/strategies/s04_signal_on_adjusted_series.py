"""Take the signal from a back-adjusted series, execute on the raw contract.

This is the separation that makes continuous futures safe. The adjusted series is continuous, so a
moving average over it is meaningful; the price you actually fill at is the live contract's own.
Mixing them up -- filling at an adjusted price -- is the classic silent futures bug.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"],
                 "description": "Signal from the adjusted series, fills on the raw contract"}
WINDOW = 20


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return

    # History of the *chain*: continuous across rolls, so the average is not broken by contract gaps.
    history = await data.history(assets=[context.chain], bar_count=WINDOW + 1,
                                 fields=["close"], frequency="1d")
    closes = history["close"].to_list() if len(history) else []
    if len(closes) < WINDOW + 1 or any(c is None for c in closes):
        return

    average = sum(closes[:-1]) / WINDOW
    wants_long = closes[-1] > average

    if context.held is not None and context.held.sid != contract.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None

    if wants_long and context.held is None:
        await context.order(asset=contract, amount=1, style=MarketOrder())
        context.held = contract
    elif not wants_long and context.held is not None:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
