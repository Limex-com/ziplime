"""Buy a coupon-bearing bond once and collect every coupon.

The baseline bond pattern, and the reference every other example is read against. Nothing is
traded after the first session: all of the cash flow after it comes from the schedule -- a coupon
every six months, credited on its payment date because the position was there on the record date.

Watch the portfolio: it steps up on each coupon date and drifts with the clean price in between.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLB26"],
    "description": "Buy a fixed-coupon bond and hold it, collecting every coupon",
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("ZLB26@MISX")
    context.bought = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.bought:
        return
    price = await data.current(assets=[context.bond], fields=["price"])
    quoted = price["price"][0]
    if quoted is None:
        return

    # A bond quote is a percentage of face value and the buyer also owes accrued interest, so the
    # money one bond costs is neither the quote nor the nominal. Ask for it rather than guess.
    per_bond = await context.bond_dirty_price(context.bond, quoted)
    amount = int(context.portfolio.cash * 0.9 / per_bond)
    if amount <= 0:
        return

    await context.order(asset=context.bond, amount=amount, style=MarketOrder())
    context.bought = True
