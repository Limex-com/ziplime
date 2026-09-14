"""A 1x2 call ratio spread: long one, short two further out. The one with no floor.

Buy an at-the-money call, sell two above it. The two short legs usually more than pay for the
long one, so the structure is opened for a credit and profits over a band -- and then, above the
short strike, loses at twice the rate of the underlying, for ever.

**`max_loss` is `-inf`, and that is not a formality.** Every other structure here reports a
number. This one reports negative infinity because the short wing is naked: one extra short call
beyond what the long leg covers, and nothing above it. The example prints that value rather than
hiding it, and refuses to open unless the caller has set `ACCEPT_UNBOUNDED`, because a structure
whose worst case cannot be stated should not be openable by forgetting to look.

**The rounding caveat is the second reason this file exists.** `OptionStrategy.orders` sizes each
leg by `round(ratio * quantity)` and *drops* a leg that rounds to zero, keeping the rest. At
`QUANTITY = 1` the legs are 1 and -2 and nothing rounds away -- but a fractional quantity can
leave the long leg behind and hand you two naked short calls under a ratio spread's name. The
shape is therefore checked against what was asked for rather than assumed from the constructor.

**Why a credit, and why check it.** The whole appeal is being paid to take the position on. If the
structure prices at a debit the two short legs did not cover the long one, which on a chain means
the upper strike barely trades -- and a naked short wing on an illiquid strike is the worst
version of this trade rather than a cheaper one.
"""
import datetime as dt
import math

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import ratio_spread

#: Expiry to trade. Must be one the chain still lists.
EXPIRY = dt.date(2026, 10, 16)
#: Strikes from the long leg out to the short one.
WIDTH_STEPS = 3
#: How many are sold per one bought. Two is the classic; anything above one is naked somewhere.
SHORT_RATIO = 2.0
#: Refuse a debit: being paid is the entire point of the structure.
MINIMUM_CREDIT = 0.10
#: The structure cannot state a worst case. Opening it is therefore a deliberate act rather than
#: a default, and flipping this to False is how the example refuses itself.
ACCEPT_UNBOUNDED = True
QUANTITY = 1

WARMUP = 0


async def initialize(context):
    context.underlying = await context.symbol("AAPL", mic="XNGS")
    context.holding = None
    context.done = False


async def handle_data(context, data):
    if context.holding is not None or context.done:
        return

    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return

    chain = await context.option_chain(context.underlying, expiration_date=EXPIRY)
    calls = sorted(chain.calls, key=lambda listing: listing.asset.strike)
    long_leg = chain.nearest_strike(float(price), OptionType.CALL)
    if long_leg is None:
        return
    strikes = [listing.asset.strike for listing in calls]
    position = strikes.index(long_leg.asset.strike)
    if position + WIDTH_STEPS >= len(calls):
        return
    short_leg = calls[position + WIDTH_STEPS]

    structure = ratio_spread(long_listing=long_leg, short_listing=short_leg,
                             short_ratio=SHORT_RATIO)
    premiums = await _price_legs(data, structure)
    if premiums is None:
        return
    net = structure.net_premium(premiums)
    if net > -MINIMUM_CREDIT:
        return

    worst = structure.max_loss(net)
    context.entry_report = {
        "name": structure.name,
        "net": net,
        "breakevens": structure.breakevens(net),
        "max_profit": structure.max_profit(net),
        "max_loss": worst,
    }
    if math.isinf(worst) and not ACCEPT_UNBOUNDED:
        context.refused = {"name": structure.name, "reason": "max_loss is unbounded"}
        return

    orders = structure.orders(QUANTITY)
    # Two legs, and the short one has to be `SHORT_RATIO` times the long. `orders` drops a leg
    # that rounds to zero and keeps the rest, which would leave naked short calls here.
    if len(orders) != 2 or orders[1][1] != -round(SHORT_RATIO * QUANTITY):
        context.refused = {"name": structure.name,
                           "reason": f"sizing produced {orders}, which is not the structure"}
        return
    for listing, amount in orders:
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.holding = orders
    context.done = True


async def _price_legs(data, structure) -> dict[int, float] | None:
    """Premium per unit for every leg, keyed by sid -- what `net_premium` wants."""
    prices = {}
    for leg in structure.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return None
        prices[leg.listing.sid] = float(quote["close"][-1])
    return prices
