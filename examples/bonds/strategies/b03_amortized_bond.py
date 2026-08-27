"""Hold an amortizing bond and watch the principal come back in instalments.

An amortizing issue repays part of its nominal every year. Two things follow, and both are visible
in the run:

* the coupon shrinks, because it is computed on the principal still outstanding;
* the same *quote* is worth less money, because it is a percentage of a smaller nominal.

The algorithm reports both each time the outstanding nominal changes, so the mechanism is legible
rather than implied by the equity curve.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLA27"],
    "description": "Hold an amortizing bond; principal returns in instalments and coupons shrink",
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("ZLA27@MISX")
    context.bought = False
    context.last_face_value = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    price = await data.current(assets=[context.bond], fields=["price"])
    quoted = price["price"][0]
    if quoted is None:
        return

    face_value = await context.bond_face_value(context.bond)
    if face_value != context.last_face_value:
        clean = await context.bond_dirty_price(context.bond, quoted)
        print(f"{context.simulation_dt.date()} outstanding nominal {face_value:>7.2f}, "
              f"a {quoted:.2f} quote is worth {clean:.2f}")
        context.last_face_value = face_value

    if context.bought:
        return
    per_bond = await context.bond_dirty_price(context.bond, quoted)
    amount = int(context.portfolio.cash * 0.9 / per_bond)
    if amount <= 0:
        return
    await context.order(asset=context.bond, amount=amount, style=MarketOrder())
    context.bought = True
