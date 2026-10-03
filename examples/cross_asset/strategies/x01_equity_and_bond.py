"""Hold shares and a long bond side by side: the classic 60/40.

The cross-asset case that catches a quotation bug immediately. Both instruments trade on the same
venue, in the same currency, on the same calendar -- and the *only* difference in how they are
handled is that the equity's quote is money per share while the bond's is a percentage of a
1000-unit nominal.

Split by money, not by count. Sizing by count would put ten times more into the bond than intended,
which is exactly the mistake the two-line difference below exists to avoid.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "equities": ["JNJ"],
    "bonds": ["ZLB26"],
    "description": "60/40 equity and a long bond, split by money",
    "equity_weight": 0.6,
    "bond_weight": 0.4,
}


async def initialize(context: TradingAlgorithm):
    context.equity = await context.symbol("JNJ@XNYS")
    context.bond = await context.bond_symbol("ZLB26@XNYS")
    context.built = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.built:
        return
    # Keyed by sid, never by position: data.current takes a set, so the order of its rows is not
    # the order of the request, and an asset with no bar today is simply absent from the result.
    quotes = await data.current(assets=[context.equity, context.bond], fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    equity_quote = prices.get(context.equity.sid)
    bond_quote = prices.get(context.bond.sid)
    if equity_quote is None or bond_quote is None:
        return

    cash = context.portfolio.cash * 0.95
    # An equity quote is what one share costs.
    equity_amount = int(cash * STRATEGY_INFO["equity_weight"] / equity_quote)
    # A bond quote is a percentage of face value, and the buyer owes accrued interest on top.
    per_bond = await context.bond_dirty_price(context.bond, bond_quote)
    bond_amount = int(cash * STRATEGY_INFO["bond_weight"] / per_bond)

    print(f"{context.simulation_dt.date()} "
          f"{context.equity.symbol} {equity_amount} @ {equity_quote:.2f} "
          f"= {equity_amount * equity_quote:,.0f} | "
          f"{context.bond.symbol} {bond_amount} @ {bond_quote:.2f}% "
          f"= {bond_amount * per_bond:,.0f}")

    await context.order(asset=context.equity, amount=equity_amount, style=MarketOrder())
    await context.order(asset=context.bond, amount=bond_amount, style=MarketOrder())
    context.built = True
