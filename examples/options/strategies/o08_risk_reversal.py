"""A risk reversal: own the upside, pay for it by selling the downside.

Long an out-of-the-money call, short an out-of-the-money put, same expiry. The two premiums
roughly cancel, so the structure costs almost nothing to put on and behaves like owning the
underlying between the strikes. That "almost nothing" is the trap the example exists to show.

**This is the one with real risk, and the numbers say so out loud.** Every other structure in this
directory is defined-risk: a butterfly, a condor, a vertical spread all have a worst case you can
read off the strikes. A risk reversal keeps the entire downside of the underlying. `max_loss`
comes back as a large finite number rather than `-inf` -- finite only because a share cannot go
below zero -- and the example refuses to open unless that number is inside a budget stated up
front. A structure that costs nothing and can lose everything is precisely the shape that gets
sized as though it were free.

**Entered on a trend, because the view is directional.** Unlike the pin structures, there is no
"where" here -- only "up". The fast average above the slow one is the plainest statement of that,
and it is computed once across the window rather than on every bar.

**Delta selection is by strike distance, not by delta.** The chain from a free source carries no
greeks worth trusting, so the legs are picked a fixed number of strikes either side of spot. That
is cruder than selecting on delta and it is stated rather than dressed up: two contracts the same
distance from the money are not equally likely to be exercised when the skew is steep, which on
equity puts it usually is.
"""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import risk_reversal

#: Expiry to trade. Must be one the chain still lists.
EXPIRY = dt.date(2026, 10, 16)
#: Strikes out from the money for each leg. Wider is cheaper and less like owning the underlying.
OFFSET_STEPS = 3
FAST, SLOW = 3, 10
#: The most this structure may be allowed to lose at expiry, in money. Checked against
#: `max_loss` before anything is ordered, because the premium says nothing about the risk here.
LOSS_BUDGET = 25_000.0
QUANTITY = 1

WARMUP = SLOW


async def initialize(context):
    context.underlying = await context.symbol("AAPL", mic="XNGS")
    context.universe = {"AAPL": context.underlying}
    context.holding = None
    context.done = False


def compute_signals(context, prices):
    """Two trailing averages, in one pass. The cross is the whole directional view."""
    close = prices.close
    return {"fast": close.rolling(FAST).mean(), "slow": close.rolling(SLOW).mean()}


async def handle_data(context, data):
    if context.holding is not None or context.done:
        return
    if not (context.signals.is_ready("fast") and context.signals.is_ready("slow")):
        return
    if float(context.signals["fast"]["AAPL"]) <= float(context.signals["slow"]["AAPL"]):
        return

    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return

    chain = await context.option_chain(context.underlying, expiration_date=EXPIRY)
    strikes = chain.strikes
    if not strikes:
        return
    middle = min(range(len(strikes)), key=lambda index: abs(strikes[index] - float(price)))
    if middle - OFFSET_STEPS < 0 or middle + OFFSET_STEPS >= len(strikes):
        return
    long_call = chain.at_strike(strikes[middle + OFFSET_STEPS], OptionType.CALL)
    short_put = chain.at_strike(strikes[middle - OFFSET_STEPS], OptionType.PUT)
    if long_call is None or short_put is None:
        return

    structure = risk_reversal(long_call=long_call, short_put=short_put)
    premiums = await _price_legs(data, structure)
    if premiums is None:
        return
    net = structure.net_premium(premiums)
    worst = structure.max_loss(net)

    # The check this structure exists to demonstrate. `net` is near zero by construction and says
    # nothing about what is at stake; `worst` is what is actually being risked.
    if worst < -LOSS_BUDGET:
        context.refused = {"name": structure.name, "max_loss": worst, "budget": -LOSS_BUDGET}
        return

    context.entry_report = {
        "name": structure.name,
        "net": net,
        "breakevens": structure.breakevens(net),
        "max_profit": structure.max_profit(net),
        "max_loss": worst,
    }
    orders = structure.orders(QUANTITY)
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
