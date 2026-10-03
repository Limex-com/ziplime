"""A short iron butterfly: the same view as the long butterfly, paid the other way round.

Sell the at-the-money straddle, buy a wing either side to cap it. The peak is again at the body
strike, so this expresses exactly what ``o06_long_butterfly`` does -- "it finishes right about
here" -- and the pair is worth reading together, because the difference between them is not the
opinion but the cash flow and what happens when the opinion is wrong.

    o06   pay a debit      profit if it pins     lose the debit if it does not
    o07   take a credit    keep it if it pins    lose up to the wing width if it does not

**Why both, when one would demonstrate the constructor.** Because the interesting part is not the
constructor. A credit structure books cash *in* at entry, which means the ledger has to be right
about a short option being an obligation rather than an asset -- and a run that only ever bought
things would never exercise that. The maximum loss printed at entry is finite for exactly one
reason: the wings. Sell the straddle without them and the same view becomes unbounded.

**The credit is checked, not assumed.** An iron butterfly that costs money to put on is not one;
it means the wings were priced above the body, which happens on a chain where the wing strikes
barely trade. The structure is refused there rather than opened at a debit under a credit
structure's name.
"""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import iron_butterfly

#: Expiry to trade. Must be one the chain still lists.
EXPIRY = dt.date(2026, 10, 16)
#: Strikes from the body out to each protective wing.
WING_STEPS = 2
#: Refuse the structure if the credit is thinner than this per unit. A few cents of credit for the
#: wing width in risk is a trade that is only ever worth it to the person on the other side.
MINIMUM_CREDIT = 0.50
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
    body_call = chain.nearest_strike(float(price), OptionType.CALL)
    if body_call is None:
        return
    body_strike = body_call.asset.strike
    # The short legs share a strike -- that is what separates an iron butterfly from an iron
    # condor -- so the put has to be at the call's strike and not merely near it.
    body_put = chain.at_strike(body_strike, OptionType.PUT)
    if body_put is None:
        return

    strikes = chain.strikes
    middle = strikes.index(body_strike)
    if middle - WING_STEPS < 0 or middle + WING_STEPS >= len(strikes):
        return
    long_put = chain.at_strike(strikes[middle - WING_STEPS], OptionType.PUT)
    long_call = chain.at_strike(strikes[middle + WING_STEPS], OptionType.CALL)
    if long_put is None or long_call is None:
        return

    structure = iron_butterfly(long_put=long_put, short_put=body_put,
                               short_call=body_call, long_call=long_call)
    premiums = await _price_legs(data, structure)
    if premiums is None:
        return

    net = structure.net_premium(premiums)
    # Negative is a credit. A positive number here means the wings cost more than the body paid,
    # which is a debit structure wearing a credit structure's name.
    if net > -MINIMUM_CREDIT:
        return

    context.entry_report = {
        "name": structure.name,
        "net": net,
        "breakevens": structure.breakevens(net),
        "max_profit": structure.max_profit(net),
        "max_loss": structure.max_loss(net),
    }

    orders = structure.orders(QUANTITY)
    if len(orders) != 4:
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
