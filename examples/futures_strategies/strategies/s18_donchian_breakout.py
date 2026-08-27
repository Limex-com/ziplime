"""Buy new highs, sell new lows: a Donchian channel breakout.

The classic futures trend rule. Highs and lows come from the adjusted chain so that a roll gap
cannot count as a breakout.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Donchian channel breakout, long and short"}
CHANNEL = 30


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.direction = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    contract = await data.current_contract(context.chain)
    if contract is None:
        return
    history = await data.history(assets=[context.chain], bar_count=CHANNEL + 1,
                                 fields=["high", "low", "close"], frequency="1d")
    if len(history) < CHANNEL + 1:
        return
    highs = history["high"].to_list()[:-1]
    lows = history["low"].to_list()[:-1]
    closes = history["close"].to_list()
    if any(v is None for v in highs + lows) or not closes[-1]:
        return

    wanted = context.direction
    if closes[-1] >= max(highs):
        wanted = 1
    elif closes[-1] <= min(lows):
        wanted = -1

    rolled = context.held is not None and context.held.sid != contract.sid
    if context.held is not None and (rolled or wanted != context.direction):
        await context.order(asset=context.held, amount=-context.direction, style=MarketOrder())
        context.held, context.direction = None, 0
    if context.held is None and wanted:
        await context.order(asset=contract, amount=wanted, style=MarketOrder())
        context.held, context.direction = contract, wanted
