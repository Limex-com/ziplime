"""Sell a 0DTE iron condor each morning and let it expire.

The structure every 0DTE premium seller starts with: short a put spread and a call spread around
the money, long wings outside them, every leg expiring at this afternoon's close. It is opened
once a session and never managed, which is the point -- what is being exercised here is the
machinery, not a trading idea:

* a chain listed this morning and gone by tonight, reached with ``context.option_chain()``;
* strike selection relative to the money rather than by name, because the names change daily;
* a four-leg structure built and sized in one place, with its risk known before it is opened;
* settlement at intrinsic value at the close, which the engine does without being asked.

The premium collected is a number the generator chose. See the directory README.
"""
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options import strategies as option_strategies

STRATEGY_INFO = {
    "description": "Sell a 0DTE iron condor at the open and let it settle at the close",
    "sessions": 5,
}

#: Strikes out of the money for the short legs, and for the long wings that cap the risk.
BODY_STEPS = 2
WING_STEPS = 5

#: Never put more than this fraction of the account at risk in one session. The condor's maximum
#: loss is known before it is opened, which is what makes sizing on risk possible at all.
RISK_BUDGET = 0.02


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")
    context.opened_on = None
    context.log = []


async def handle_data(context, data):
    session = context.get_datetime().date()
    if context.opened_on == session:
        return

    # The chain is listed this morning and expires this afternoon. Asking for it every session is
    # not a caching failure: yesterday's contracts no longer exist.
    chain = await context.option_chain(context.underlying)

    spot_frame = await data.current(assets=[context.underlying], fields=["close"])
    if spot_frame.is_empty():
        return
    spot = float(spot_frame["close"][-1])

    condor = option_strategies.condor_around(chain, spot, body_steps=BODY_STEPS,
                                             wing_steps=WING_STEPS)

    prices = {}
    for leg in condor.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return  # a leg with no market: no structure
        prices[leg.listing.sid] = float(quote["close"][-1])

    net = condor.net_premium(prices)
    if net >= 0:
        return  # a condor that costs money to put on is not the trade

    worst = condor.max_loss(net)
    budget = context.portfolio.portfolio_value * RISK_BUDGET
    quantity = int(budget // abs(worst)) if worst else 0
    if quantity < 1:
        return

    for listing, amount in condor.orders(quantity):
        await context.order(asset=listing, amount=amount, style=MarketOrder())

    context.opened_on = session
    context.log.append({
        "session": session, "spot": spot, "structure": condor.name, "quantity": quantity,
        "credit": -net * quantity, "max_loss": worst * quantity,
        "breakevens": condor.breakevens(net),
    })
