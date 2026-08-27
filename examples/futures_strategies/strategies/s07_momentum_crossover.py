"""Fast/slow moving-average crossover on the continuous series.

A long/short trend rule. Because the signal reads the adjusted chain, the crossover is not
triggered by a contract gap at a roll -- an unadjusted series would produce exactly that kind of
false signal every quarter.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Long/short moving-average crossover"}
FAST, SLOW = 10, 40


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.direction = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    history = await data.history(assets=[context.chain], bar_count=SLOW, fields=["close"],
                                 frequency="1d")
    closes = history["close"].to_list() if len(history) else []
    if len(closes) < SLOW or any(c is None for c in closes):
        return

    fast = sum(closes[-FAST:]) / FAST
    slow = sum(closes) / SLOW
    wanted = 1 if fast > slow else -1

    rolled = context.held is not None and context.held.sid != contract.sid
    if context.held is not None and (rolled or wanted != context.direction):
        await context.order(asset=context.held, amount=-context.direction, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=contract, amount=wanted, style=MarketOrder())
        context.held = contract
        context.direction = wanted
