"""Hold three unrelated futures markets, sized to equal exposure.

Energy, gas and grain, with multipliers of 1 000, 10 000 and 50. Equal *exposure* therefore means
very different contract counts, and sizing by count instead would put two hundred times more risk
in gas than in corn.

Every leg is a specific dated contract from the front of its own chain, so the three are real
instruments with real expiration dates rather than three stitched series.

This is the case that makes the multiplier concrete. It is also the case where a wrong one hides
best, because every position still looks plausible on its own.

Two things about real dated contracts show up here that a single stitched series would hide:

* The target is **worked over several sessions** rather than sent once. These contracts are thin,
  fills are capped at a share of each bar's volume, and Yahoo reports no volume at all on many
  sessions -- so a single order leaves the basket lopsided, which is not an equal-exposure basket.
  The number of days it took to complete is printed below. Topping a leg up means checking
  ``get_open_orders`` first: an unfilled order is not in the position, so reordering the shortfall
  every session stacks orders and buys a multiple of the target.
* The S&P chain is deliberately absent. Its dated contracts price on every session but report
  volume on almost none, so an index leg would sit unfilled for months while the others traded.
  ``f05_term_structure`` reads the S&P curve without trading it.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["CL", "NG", "ZC"],
    "description": "Three markets at equal exposure; the multipliers differ by 200x",
    "margin": "fixed",
}


async def initialize(context: TradingAlgorithm):
    context.chains = {root: await context.futures_chain(root)
                      for root in STRATEGY_INFO["roots"]}
    context.targets = None
    context.started_on = None
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    if context.reported:
        return

    if context.targets is None:
        # The front contract of each chain that is actually priced on this session. A contract
        # that had not listed yet when the window opened is forward-filled with zeros until its
        # first real bar, so chain[0] is not always the front contract that is trading.
        everything = [c for chain in context.chains.values() for c in chain]
        quotes = await data.current(assets=everything, fields=["price"])
        prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))
        legs = []
        for chain in context.chains.values():
            front = next((c for c in chain if prices.get(c.sid) and prices[c.sid] > 0), None)
            if front is None:
                return  # this market has no priced contract yet; wait for one
            legs.append(front)

        per_market = context.portfolio.portfolio_value / len(legs)
        context.targets = {
            leg: context.contracts_for_notional(asset=leg, notional=per_market,
                                                price=prices[leg.sid])
            for leg in legs
        }
        context.started_on = context.simulation_dt.date()

    quotes = await data.current(assets=list(context.targets), fields=["price"])
    prices = dict(zip(quotes["sid"].to_list(), quotes["price"].to_list()))

    held, short_by = {}, {}
    for leg, target in context.targets.items():
        amount = await context.portfolio.get_asset_positions_amount(leg)
        held[leg] = amount
        short_by[leg] = target - amount

    if any(short_by.values()):
        for leg, missing in short_by.items():
            price = prices.get(leg.sid)
            if not missing or not price or price <= 0:
                continue
            # Only top up a leg with nothing already working. The order from a previous session
            # stays open until it fills, and it is not reflected in the position -- so sending the
            # shortfall again every session stacks order on order and buys many times the target.
            if context.get_open_orders(leg):
                continue
            await context.order(asset=leg, amount=missing, style=MarketOrder())
        return

    context.reported = True
    sessions = (context.simulation_dt.date() - context.started_on).days
    print(f"{context.simulation_dt.date()} basket complete, {sessions} calendar days after the "
          f"first order:")
    for leg, amount in held.items():
        price = prices[leg.sid]
        print(f"  {leg.symbol:12s} {amount:>4d} @ {price:>10,.3f} "
              f"multiplier {leg.asset.multiplier:>8,.0f} "
              f"= {context.notional_exposure(leg, amount, price):>12,.0f}")
    print(f"  {'total exposure':12s} {abs(context.portfolio.positions_exposure):>44,.0f}")
    print(f"  {'position value':12s} {context.portfolio.positions_value:>44,.0f}"
          f"   <- futures carry none")
