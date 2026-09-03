"""Watch a futures position settle its variation every day.

An equity's profit sits in the position until it is sold. A future's does not: the exchange moves
cash between the two sides every day, so the position carries no value and the whole result is in
the cash balance.

This holds a dated gas contract and prints each new largest settlement in either direction. Those
are real cash movements, and their size -- a multiplier times a price move -- is why a futures
backtest that treats the position as an equity is not merely imprecise but structurally wrong. One
cent on natural gas is 100 dollars a contract.

The settlement figures are only meaningful once the position is actually on, so the daily
comparison starts from the session the fill is observed rather than the session the order was
placed: on this data those can be several sessions apart.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["NG"],
    "description": "Hold gas contracts; P&L settles daily into cash, not into position value",
}

CONTRACTS = 5


async def initialize(context: TradingAlgorithm):
    chain = await context.futures_chain("NG")
    context.contract = chain[0]
    context.ordered = False
    context.previous_cash = None
    context.largest_gain = 0.0
    context.largest_loss = 0.0
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    quotes = await data.current(assets=[context.contract], fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    price = prices.get(context.contract.sid)
    # A contract that had not listed yet when the window opened is forward-filled with zeros
    # until its first real bar, and zero is not a price.
    if not price or price <= 0:
        return

    if not context.ordered:
        await context.order(asset=context.contract, amount=CONTRACTS, style=MarketOrder())
        context.ordered = True
        return

    held = await context.portfolio.get_asset_positions_amount(context.contract)
    if not held:
        return

    if not context.reported:
        context.reported = True
        context.previous_cash = context.portfolio.cash
        print(f"{context.simulation_dt.date()} holding {held} x "
              f"{context.contract.symbol} @ {price:.3f}")
        print(f"  position value    {context.portfolio.positions_value:>12,.0f}"
              f"   <- a future carries none")
        print(f"  exposure          {context.portfolio.positions_exposure:>12,.0f}"
              f"   <- price x multiplier x quantity")
        return

    settled = context.portfolio.cash - context.previous_cash
    context.previous_cash = context.portfolio.cash
    if settled > context.largest_gain:
        context.largest_gain = settled
        print(f"{context.simulation_dt.date()} settled +{settled:>10,.0f} "
              f"at {price:.3f} - a new largest gain")
    elif settled < context.largest_loss:
        context.largest_loss = settled
        print(f"{context.simulation_dt.date()} settled {settled:>11,.0f} "
              f"at {price:.3f} - a new largest loss")
