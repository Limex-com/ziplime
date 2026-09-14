"""A defined-risk bull call spread on real Yahoo option data, held with a stop.

The structure is the point rather than the edge. A vertical spread is two legs that have to be
opened together, marked together and closed together, and it is the cheapest way to find out
whether an option *structure* -- not a single contract -- survives the round trip through the
chain, the ledger and the metric set intact.

**Why a spread rather than a single call.** One leg tests almost nothing: a long call is a
quantity of something with a price, which is the shape the engine handles for every instrument.
Two legs of the same expiry and opposite sign exercise the parts that are actually option-specific
-- a short leg that takes in premium rather than paying it, a multiplier applied on both sides,
and a net debit that has to come out right when the legs are marked independently.

**The window is days, not years, and that is Yahoo's limit rather than a design choice.** Daily
history for a listed contract reaches back about a month and only for contracts listed that long;
minute history is the last session or two. See
:mod:`ziplime.data.data_sources.options.yahoo_chain`.
"""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import vertical_spread

#: Expiry to trade. Has to be one Yahoo still lists -- an expiry already past is not in the chain.
EXPIRY = dt.date(2026, 10, 16)
#: How far above the long strike the short leg sits, in strikes. Wider is more premium paid and
#: more to gain; one step is the tightest spread the chain allows.
WIDTH_STEPS = 2
#: Close the structure if the underlying falls this far below where it was entered. A defined-risk
#: spread cannot lose more than its debit, so this is about ending the trade rather than capping
#: the loss -- the cap is already in the structure.
STOP = 0.02
#: Spreads to buy. One is 100 shares of exposure per leg.
QUANTITY = 1

#: Sessions of history before the run's first bar. None needed: the entry reads a price, not a
#: window, and the stop measures from the entry rather than from an average.
WARMUP = 0


async def initialize(context):
    context.underlying = await context.symbol("AAPL", mic="XNGS")
    context.holding = None
    context.entry_price = None
    context.done = False


async def handle_data(context, data):
    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return
    price = float(price)

    if context.holding is None:
        if context.done:
            return
        await _open_spread(context, data, price)
        return

    # Held. One way out, and it is a decision about the underlying rather than about the legs:
    # marking the spread itself would mean pricing two contracts to decide about two contracts.
    if price <= context.entry_price * (1.0 - STOP):
        await _close(context)


async def _open_spread(context, data, price: float):
    chain = await context.option_chain(context.underlying, expiration_date=EXPIRY)
    calls = sorted(chain.calls, key=lambda listing: listing.asset.strike)
    if len(calls) < WIDTH_STEPS + 1:
        return

    long_leg = chain.nearest_strike(price, OptionType.CALL)
    if long_leg is None:
        return
    strikes = [listing.asset.strike for listing in calls]
    position = strikes.index(long_leg.asset.strike)
    if position + WIDTH_STEPS >= len(calls):
        # Not enough strikes above the money for the width asked for. Narrowing it silently would
        # trade a different structure than the one this file describes.
        return
    short_leg = calls[position + WIDTH_STEPS]

    quotes = await data.current(assets=[long_leg, short_leg], fields=["close"])
    if any(value is None for value in quotes["close"].to_list()):
        return

    structure = vertical_spread(long_listing=long_leg, short_listing=short_leg)
    premiums = {}
    for leg in structure.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return
        premiums[leg.listing.sid] = float(quote["close"][-1])
    net = structure.net_premium(premiums)
    context.entry_report = {
        "name": structure.name,
        "net": net,
        "breakevens": structure.breakevens(net),
        "max_profit": structure.max_profit(net),
        "max_loss": structure.max_loss(net),
    }

    orders = structure.orders(QUANTITY)
    for listing, amount in orders:
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.holding = orders
    context.entry_price = price
    context.structure_name = structure.name


async def _close(context):
    for listing, amount in context.holding:
        await context.order(asset=listing, amount=-amount, style=MarketOrder())
    context.holding = None
    context.done = True
