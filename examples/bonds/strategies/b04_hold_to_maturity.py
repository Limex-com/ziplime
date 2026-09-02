"""Hold a short-dated bond through its redemption, then sit in cash.

Redemption is not a trade. On the maturity date the issuer repays the outstanding nominal, whatever
the last quote happened to be, and the position disappears -- so a backtest that closes it at the
final bar books a gain or loss that never happened.

``ZLS25`` matures inside the example window, so the run covers the whole life: buy, collect
coupons, get repaid at par, hold cash to the end. The printed line at redemption is the moment the
position turns back into money.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "tickers": ["ZLS25"],
    "description": "Hold a bond through maturity; the issuer repays par and the position ends",
}


async def initialize(context: TradingAlgorithm):
    context.bond = await context.bond_symbol("ZLS25@XNYS")
    context.bought = False
    context.reported_redemption = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    held = await context.portfolio.get_asset_positions_amount(context.bond)

    if context.bought and held == 0 and not context.reported_redemption:
        print(f"{context.simulation_dt.date()} redeemed; cash is now "
              f"{context.portfolio.cash:,.2f}")
        context.reported_redemption = True
        return

    if context.bought:
        return
    price = await data.current(assets=[context.bond], fields=["price"])
    quoted = price["price"][0]
    if quoted is None:
        return

    ytm = await context.bond_yield_to_maturity(context.bond, quoted)
    per_bond = await context.bond_dirty_price(context.bond, quoted)
    amount = int(context.portfolio.cash * 0.9 / per_bond)
    if amount <= 0:
        return
    print(f"{context.simulation_dt.date()} buying {amount} at {quoted:.2f} "
          f"({per_bond:,.2f} each, simple yield to maturity {ytm:.2%})")
    await context.order(asset=context.bond, amount=amount, style=MarketOrder())
    context.bought = True
