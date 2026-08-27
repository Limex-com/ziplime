"""Long one single-stock future against another: a relative-value pair.

Sberbank against Gazprom, matched by notional rather than by contract count so the pair is roughly
market neutral. Both legs roll on their own schedules, which is why each is tracked separately.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["SR", "GZ"],
                 "description": "Notional-matched pair: long Sberbank, short Gazprom"}
LEG_NOTIONAL = 300_000.0
#: Only re-size when the target moves by this many contracts.
REBALANCE_BAND = 2


async def initialize(context: TradingAlgorithm):
    context.long_chain = await context.continuous_future("SR", offset=0, roll="volume",
                                                         adjustment="mul")
    context.short_chain = await context.continuous_future("GZ", offset=0, roll="volume",
                                                          adjustment="mul")
    context.held = {}


async def _rebalance(context, data, chain, sign):
    contract = await data.current_contract(chain)
    if contract is None:
        return
    current = await data.current(assets=[contract], fields=["close"])
    if len(current) == 0 or not current["close"][0]:
        return
    wanted = sign * context.contracts_for_notional(asset=contract, notional=LEG_NOTIONAL,
                                                   price=current["close"][0])
    held, amount = context.held.get(chain.root_symbol, (None, 0))
    if held is not None and held.sid != contract.sid:
        await context.order(asset=held, amount=-amount, style=MarketOrder())
        held, amount = None, 0
    if held is None:
        if wanted:
            await context.order(asset=contract, amount=wanted, style=MarketOrder())
            context.held[chain.root_symbol] = (contract, wanted)
    elif abs(wanted - amount) >= REBALANCE_BAND:
        await context.order(asset=contract, amount=wanted - amount, style=MarketOrder())
        context.held[chain.root_symbol] = (contract, wanted)


async def handle_data(context: TradingAlgorithm, data: BarData):
    await _rebalance(context, data, context.long_chain, 1)
    await _rebalance(context, data, context.short_chain, -1)
