"""Hold four chains at once, each sized to the same notional.

Every root has its own multiplier and its own roll schedule, so equal weight means equal notional,
not an equal number of contracts. All four are quoted in roubles -- mixing in a USD-quoted root
such as RTS or Brent would add up P&L in two currencies.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {"roots": ["Si", "SR", "GZ", "MX"],
                 "description": "Equal-notional basket across four roots"}
NOTIONAL_PER_ROOT = 250_000.0
#: Rebalance only when the target moves by at least this many contracts. Without a band, a target
#: that drifts by one contract a day turns into a trade a day in every root.
REBALANCE_BAND = 2


async def initialize(context: TradingAlgorithm):
    context.chains = [await context.continuous_future(root, offset=0, roll="volume",
                                                      adjustment="mul")
                      for root in ("Si", "SR", "GZ", "MX")]
    context.held = {}


async def handle_data(context: TradingAlgorithm, data: BarData):
    for chain in context.chains:
        contract = await data.current_contract(chain)
        if contract is None:
            continue
        current = await data.current(assets=[contract], fields=["close"])
        if len(current) == 0 or not current["close"][0]:
            continue
        wanted = context.contracts_for_notional(asset=contract, notional=NOTIONAL_PER_ROOT,
                                                price=current["close"][0])
        held, amount = context.held.get(chain.root_symbol, (None, 0))

        if held is not None and held.sid != contract.sid:
            # A roll is always a full close and reopen: the old contract stops existing.
            await context.order(asset=held, amount=-amount, style=MarketOrder())
            held, amount = None, 0

        if held is None:
            if wanted:
                await context.order(asset=contract, amount=wanted, style=MarketOrder())
                context.held[chain.root_symbol] = (contract, wanted)
        elif abs(wanted - amount) >= REBALANCE_BAND:
            # Otherwise adjust by the difference rather than churning the whole position.
            await context.order(asset=contract, amount=wanted - amount, style=MarketOrder())
            context.held[chain.root_symbol] = (contract, wanted)
