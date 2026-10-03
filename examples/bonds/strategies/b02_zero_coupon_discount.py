"""Hold a zero-coupon bond: the same trade with no payouts at all.

The control case for b01. Same dates, same holding period, same buy-and-hold logic -- but this
issue pays nothing until it is redeemed, so all of the return comes from the clean price pulling
towards par. Run it next to b01 and the difference between the two is exactly what the
coupon schedule contributes.

It is also the case that catches a bond backtest quietly paying phantom coupons: if this one
shows periodic cash credits, something is generating a schedule that does not exist.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLZ26"],
    "description": "Hold a zero-coupon bond -- no payouts, the return is the pull to par",
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("ZLZ26@XNYS")
    context.bought = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.bought:
        return
    price = await data.current(assets=[context.bond], fields=["price"])
    quoted = price["price"][0]
    if quoted is None:
        return

    # A zero-coupon bond accrues nothing, so the dirty price equals the clean one. Asking anyway
    # keeps the sizing identical to b01, which is what makes the two runs comparable.
    per_bond = await context.bond_dirty_price(context.bond, quoted)
    amount = int(context.portfolio.cash * 0.9 / per_bond)
    if amount <= 0:
        return

    await context.order(asset=context.bond, amount=amount, style=MarketOrder())
    context.bought = True
