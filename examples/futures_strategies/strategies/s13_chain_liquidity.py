"""Pick the most actively traded contract in the chain rather than assuming the front month.

``data.current_chain`` returns the live contracts in order. Around a roll the front month can
already be quieter than the next one, and this rule follows the volume instead of the calendar.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Hold whichever chain contract trades the most"}


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    contracts = (await data.current_chain(context.chain))[:3]
    if not contracts:
        return
    quotes = await data.current(assets=contracts, fields=["volume"])
    if len(quotes) == 0:
        return
    volume_by_sid = dict(zip(quotes["sid"].to_list(), quotes["volume"].to_list()))
    best = max(contracts, key=lambda c: volume_by_sid.get(c.sid) or 0.0)
    if not volume_by_sid.get(best.sid):
        return

    if context.held is not None and context.held.sid != best.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
    if context.held is None:
        await context.order(asset=best, amount=1, style=MarketOrder())
        context.held = best
