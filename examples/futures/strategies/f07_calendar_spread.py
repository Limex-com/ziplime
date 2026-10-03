"""Trade the shape of the curve instead of its level: long the front, short a deferred contract.

Both legs are the same commodity, so the outright price of crude very largely cancels. What is left
is the **spread** between two delivery months -- and that moves for its own reasons: storage costs,
inventories, how urgently the physical market wants barrels now rather than later.

This is also the cleanest demonstration that a futures position carries no value. Two positions,
several hundred thousand dollars of gross exposure between them, and the portfolio's
`positions_value` stays at zero throughout while every day's move settles into cash. An equity
long/short book of the same size would show that gross exposure as position value.

Both legs are real dated contracts from the ingested chain, priced on the same sessions, so the
spread printed below is a spread that actually traded. The reported spread P&L is computed from the
positions the ledger holds, which is what makes it comparable to the portfolio's own result: the
two legs are thin enough that they need not fill at the same time or in the same size.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL"],
    "description": "Long the front WTI contract, short a deferred one; trade the spread alone",
    "margin": "fixed",
}

#: How far down the chain the short leg sits. Six months out on a monthly chain.
DEFERRED_OFFSET = 6
CONTRACTS = 4
#: Only report a new extreme once the spread has moved this far past the last one, so the log
#: shows the shape of the move rather than every tick of it.
REPORT_STEP = 1.0


async def initialize(context: TradingAlgorithm):
    chain = await context.futures_chain("CL")
    context.near = chain[0]
    context.far = chain[DEFERRED_OFFSET]
    context.ordered = False
    context.entry_spread = None
    context.widest = None
    context.narrowest = None


async def handle_data(context: TradingAlgorithm, data: BarData):
    legs = [context.near, context.far]
    quotes = await data.current(assets=legs, fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
    near_price, far_price = prices.get(context.near.sid), prices.get(context.far.sid)
    if not near_price or not far_price or near_price <= 0 or far_price <= 0:
        return

    if not context.ordered:
        context.ordered = True
        await context.order(asset=context.near, amount=CONTRACTS, style=MarketOrder())
        await context.order(asset=context.far, amount=-CONTRACTS, style=MarketOrder())
        return

    near_held = await context.portfolio.get_asset_positions_amount(context.near)
    far_held = await context.portfolio.get_asset_positions_amount(context.far)
    if not near_held or not far_held:
        return

    spread = near_price - far_price
    if context.entry_spread is None:
        # The first session both legs are on: this is the spread the position was actually
        # established at, whatever the two orders were sent at.
        context.entry_spread = spread
        context.widest = context.narrowest = spread
        print(f"{context.simulation_dt.date()} long {near_held} x {context.near.symbol} "
              f"@ {near_price:,.2f}, short {abs(far_held)} x {context.far.symbol} "
              f"@ {far_price:,.2f}")
        print(f"  entry spread   {spread:>+13,.2f}")
        print(f"  gross exposure {abs(context.portfolio.positions_exposure):>13,.0f}")
        print(f"  position value {context.portfolio.positions_value:>13,.0f}   <- both legs are "
              f"futures, so neither carries any")
        return

    # Matched legs only: the spread P&L is defined on the paired quantity, and a partially filled
    # book carries an outright position on top of it that is not a spread trade at all.
    matched = min(near_held, abs(far_held))
    multiplier = context.near.asset.multiplier
    moved = (spread - context.entry_spread) * multiplier * matched

    if spread > context.widest + REPORT_STEP:
        context.widest = spread
        print(f"{context.simulation_dt.date()} spread {spread:>+7,.2f} - widest so far "
              f"({moved:>+11,.0f} vs entry on {matched} matched)")
    elif spread < context.narrowest - REPORT_STEP:
        context.narrowest = spread
        print(f"{context.simulation_dt.date()} spread {spread:>+7,.2f} - narrowest so far "
              f"({moved:>+11,.0f} vs entry on {matched} matched)")
