"""Read the real term structure and trade its slope.

A futures chain is not one price, it is a curve: every contract on it trades at once, and the shape
they make is information no single series carries. Two shapes have names, and opposite meanings for
anyone holding a position through a roll:

* **Backwardation** -- deferred contracts cheaper than the front. A long position rolls *down* the
  curve into a cheaper contract each time, so carry is positive.
* **Contango** -- deferred contracts dearer. A long position rolls *up*, paying the difference at
  every roll, and that cost has nothing to do with whether the price went the way you wanted.

The curve here is real. On WTI the deferred contracts sit several dollars below the front, which is
a genuine backwardated crude curve; on the S&P they sit above it, which is not a risk premium at
all but the cost of carrying the index -- interest less dividends. Reading the two the same way is
the mistake this example exists to prevent, so it prints both curves and only trades the commodity.

The position is the plain carry trade: long the front WTI contract while the curve is backwardated,
flat when it is not, rebalanced monthly. It is a demonstration of reading the curve, not a
recommendation.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL", "ES"],
    "description": "Read the real curve; hold crude only while it is backwardated",
    "margin": "fixed",
}

#: Exposure to take when the curve says to, as a share of the portfolio.
TARGET_EXPOSURE = 0.5
#: How far out to measure the slope. Six contracts on a monthly chain is half a year.
SLOPE_HORIZON = 6


def annualised_slope(near_price: float, far_price: float, months: int) -> float:
    """Slope between two points on the curve, as a fraction of the near price, per year.

    Negative means backwardation. Annualising matters because a one-dollar gap over one month and
    the same gap over a year are not remotely the same trade.
    """
    if near_price == 0 or months == 0:
        return 0.0
    return (far_price - near_price) / near_price * (12.0 / months)


async def initialize(context: TradingAlgorithm):
    context.crude = await context.futures_chain("CL")
    context.index = await context.futures_chain("ES")
    context.last_rebalance = None
    context.printed_curves = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance is not None and (today - context.last_rebalance).days < 30:
        return

    everything = context.crude + context.index
    quotes = await data.current(assets=everything, fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))

    # A contract that had not listed yet when the window opened is forward-filled with zeros
    # until its first real bar; zero is not a point on the curve.
    curve = [(c, prices.get(c.sid)) for c in context.crude]
    curve = [(c, p) for c, p in curve if p and p > 0]
    if len(curve) <= SLOPE_HORIZON:
        return

    if not context.printed_curves:
        context.printed_curves = True
        for name, chain in (("WTI crude", context.crude), ("E-mini S&P", context.index)):
            points = [(c, prices.get(c.sid)) for c in chain]
            points = [(c, p) for c, p in points if p and p > 0]
            if len(points) < 2:
                continue
            front_price = points[0][1]
            shape = "backwardation" if points[-1][1] < front_price else "contango"
            print(f"{today} {name} curve ({shape}):")
            for contract, price in points[:8]:
                gap = (price - front_price) / front_price * 100.0
                print(f"    {contract.symbol:12s} expires {contract.asset.expiration_date}  "
                      f"{price:>9,.2f}  {gap:>+7.2f}% vs front")

    front, front_price = curve[0]
    far, far_price = curve[SLOPE_HORIZON]
    slope = annualised_slope(front_price, far_price, SLOPE_HORIZON)

    held = await context.portfolio.get_asset_positions_amount(front)
    if slope < 0:
        target = context.contracts_for_notional(
            asset=front, notional=context.portfolio.portfolio_value * TARGET_EXPOSURE,
            price=front_price)
    else:
        target = 0
    if target != held:
        await context.order(asset=front, amount=target - held, style=MarketOrder())
        print(f"{today} slope {slope:>+7.2%}/yr "
              f"({front.symbol} {front_price:,.2f} vs {far.symbol} {far_price:,.2f})"
              f" -> hold {target}")
    context.last_rebalance = today
