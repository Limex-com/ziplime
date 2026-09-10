"""Size a real dated contract by notional, and watch what it costs to open: nothing.

The first thing to understand about a futures position, and the one that catches every backtester
that treats it like a share. Opening it moves no cash -- only margin is tied up -- and the position
carries no value of its own. What it has is **exposure**: `price x multiplier x quantity`.

Cash does move once the position is on, but by the day's settlement rather than by the notional --
which is the distinction the printed lines are there to make.

The contract traded is the front of the real WTI chain, named and dated: not a stitched series, but
the specific crude contract nearest to expiry when the position opened. At around 85 dollars a
barrel one of them is 85 000 dollars of exposure, from a multiplier of 1 000 that Yahoo does not
publish anywhere.

Everything printed below is read back from the **position**, never from the order. On this data
those differ: Yahoo reports no volume on many sessions, and the simulation will not fill an order
on a bar where nothing traded, so a fill can arrive several sessions after the order. An example
that printed what it *intended* to hold would misreport the portfolio for as long as that took.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL"],
    "description": "Hold a fixed notional of a dated crude contract; opening it costs no cash",
}

#: Target exposure, as a share of the portfolio.
TARGET_EXPOSURE = 0.5


async def initialize(context: TradingAlgorithm):
    # Element 0 of the chain is the front contract: the chain is ordered by expiration.
    chain = await context.futures_chain("CL")
    context.contract = chain[0]
    context.ordered_on = None
    context.reported = False
    context.previous_cash = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    quotes = await data.current(assets=[context.contract], fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    price = prices.get(context.contract.sid)
    # A contract that had not listed yet when the window opened is forward-filled with zeros
    # until its first real bar, and zero is not a price.
    if not price or price <= 0:
        return

    held = await context.portfolio.get_asset_positions_amount(context.contract)

    if context.ordered_on is None:
        contracts = context.contracts_for_notional(
            asset=context.contract, notional=context.portfolio.portfolio_value * TARGET_EXPOSURE,
            price=price)
        if contracts <= 0:
            return
        context.ordered_on = today
        context.previous_cash = context.portfolio.cash
        await context.order(asset=context.contract, amount=contracts, style=MarketOrder())
        print(f"{today} ordered {contracts} x {context.contract.symbol} with the close at "
              f"{price:,.2f}")
        return

    if held and not context.reported:
        context.reported = True
        contract = context.contract.asset
        exposure = context.notional_exposure(asset=context.contract, amount=held, price=price)
        delay = (today - context.ordered_on).days
        print(f"{today} filled: holding {held} x {context.contract.symbol} "
              f"(expires {contract.expiration_date}), {delay} calendar days after the order")
        print(f"  exposure       {exposure:>14,.0f}   <- price x multiplier x quantity")
        print(f"  cash before    {context.previous_cash:>14,.2f}")
        print(f"  cash after     {context.portfolio.cash:>14,.2f}")
        moved = context.portfolio.cash - context.previous_cash
        print(f"  cash moved by  {moved:>14,.2f}   <- commission and one session of variation "
              f"margin, not {exposure:,.0f} of notional")
        print(f"  position value {context.portfolio.positions_value:>14,.0f}   <- a future "
              f"carries none")

    context.previous_cash = context.portfolio.cash
