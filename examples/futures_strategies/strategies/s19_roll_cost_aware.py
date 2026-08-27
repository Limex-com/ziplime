"""Measure what each roll costs before paying it.

The gap between the outgoing and incoming contract is not profit or loss -- but it is a real cost
of carrying the position forward. This rule reads both contracts out of the chain, records the gap,
and stands aside for a while when carrying the position forward is unusually expensive.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si"], "description": "Skip the roll when carry is unusually expensive"}
MAX_ROLL_COST = 0.010          # as a share of price


async def initialize(context: TradingAlgorithm):
    context.chain = await context.continuous_future("Si", offset=0, roll="volume", adjustment="mul")
    context.held = None
    context.skipping = False
    context.roll_costs = []


async def handle_data(context: TradingAlgorithm, data: BarData):
    contracts = (await data.current_chain(context.chain))[:2]
    if not contracts:
        return
    front = contracts[0]

    if context.held is not None and context.held.sid != front.sid:
        await context.order(asset=context.held, amount=-1, style=MarketOrder())
        context.held = None
        if len(contracts) > 1:
            quotes = await data.current(assets=contracts[:2], fields=["close"])
            prices = dict(zip(quotes["sid"].to_list(), quotes["close"].to_list()))
            near, far = prices.get(contracts[0].sid), prices.get(contracts[1].sid)
            if near and far:
                cost = (far - near) / near
                context.roll_costs.append(cost)
                context.skipping = cost > MAX_ROLL_COST

    if context.held is None:
        if context.skipping:
            context.skipping = False       # sit out one session, then resume
            return
        await context.order(asset=front, amount=1, style=MarketOrder())
        context.held = front
