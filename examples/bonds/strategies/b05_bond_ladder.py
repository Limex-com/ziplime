"""Split the money across bonds maturing at different dates -- a ladder.

The standard way to hold bonds without betting on rates: several issues with staggered maturities,
so principal comes back at intervals rather than all at once. Here the rungs are a 2025, two 2026s
and a 2027, sized equally by money rather than by count -- the quotes differ, so equal counts would
not be equal exposures.

The point worth checking in the output: as each rung matures the portfolio's exposure falls and
its cash rises, without a single sell order.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLS25", "ZLB26", "ZLZ26", "ZLA27"],
    "description": "Equal-money ladder across four maturities; rungs redeem one by one",
}


async def initialize(context: TradingAlgorithm):
    context.rungs = [await context.bond_symbol(f"{ticker}@MISX")
                     for ticker in STRATEGY_INFO["tickers"]]
    context.built = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.built:
        return
    prices = await data.current(assets=context.rungs, fields=["price"])
    quotes = prices["price"]
    if any(quote is None for quote in quotes):
        return

    per_rung = context.portfolio.cash * 0.9 / len(context.rungs)
    for bond, quoted in zip(context.rungs, quotes):
        per_bond = await context.bond_dirty_price(bond, quoted)
        amount = int(per_rung / per_bond)
        if amount > 0:
            await context.order(asset=bond, amount=amount, style=MarketOrder())
    context.built = True
