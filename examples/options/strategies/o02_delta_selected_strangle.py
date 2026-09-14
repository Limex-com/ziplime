"""Sell a delta-selected 0DTE strangle in the morning and close it before the gamma bites.

Two things this shows that the condor example does not.

**Selecting by delta rather than by strike.** "The 20-delta put" is how an options desk names a
strike, and it adapts to volatility in a way "two strikes out" does not: on a quiet day the 20
delta is close to the money, on a violent one it is far away. The catch on 0DTE is that this stops
working as the session runs out -- with an hour left almost every contract has a delta of 0 or 1 --
which is why the selection happens in the morning and is never repeated.

**Closing before expiry.** The condor example holds to settlement; this one flattens at a fixed
time. That is the actual 0DTE risk decision: gamma grows without bound into the close, so the last
half hour is where a position that has been fine all day becomes the whole day's P&L. The strategy
records the book's Greeks at each decision so the trajectory is visible in the result.

The prices are synthetic. See the directory README.
"""
import datetime

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options import strategies as option_strategies

STRATEGY_INFO = {
    "description": "Sell a 20-delta 0DTE strangle at the open, flatten before the close",
    "sessions": 5,
}

#: Delta to sell each wing at, as a magnitude.
TARGET_DELTA = 0.20

#: When to open, and when to be flat. Both are times of day rather than bar counts, so the same
#: strategy runs unchanged at one minute and at five.
OPEN_AFTER = datetime.time(9, 45)
FLATTEN_AT = datetime.time(15, 30)

#: Contracts per wing. Fixed, because the point here is the selection and the Greeks rather than
#: the sizing -- the condor example sizes on risk.
LOTS = 2


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")
    context.open_legs = []
    context.opened_on = None
    context.greeks_log = []


async def handle_data(context, data):
    now = context.get_datetime()
    session = now.date()

    if context.open_legs and now.time() >= FLATTEN_AT:
        await _flatten(context, data)
        return

    if context.opened_on == session or now.time() < OPEN_AFTER:
        if context.open_legs:
            await _record_greeks(context, data)
        return

    spot_frame = await data.current(assets=[context.underlying], fields=["close"])
    if spot_frame.is_empty():
        return
    spot = float(spot_frame["close"][-1])

    chain = await context.option_chain(context.underlying)
    # One contract's own implied volatility stands in for the chain's level. Reading it from the
    # data rather than assuming one keeps the selection honest when the regime changes.
    reference = chain.atm(spot, OptionType.CALL)
    years = context.time_to_expiry(reference)
    if years <= 0:
        return
    reference_greeks = await context.option_greeks(reference)
    volatility = reference_greeks.volatility

    call = chain.nearest_delta(TARGET_DELTA, OptionType.CALL, spot=spot,
                               years_to_expiry=years, volatility=volatility)
    put = chain.nearest_delta(TARGET_DELTA, OptionType.PUT, spot=spot,
                              years_to_expiry=years, volatility=volatility)
    if call.sid == put.sid:
        return

    strangle = option_strategies.strangle(call, put, ratio=-1.0)
    for listing, amount in strangle.orders(LOTS):
        await context.order(asset=listing, amount=amount, style=MarketOrder())

    context.open_legs = [(listing, amount) for listing, amount in strangle.orders(LOTS)]
    context.opened_on = session
    await _record_greeks(context, data)


async def _flatten(context, data):
    """Buy the wings back. Holding a short 0DTE strangle into the close is the whole risk."""
    for listing, amount in context.open_legs:
        await context.order(asset=listing, amount=-amount, style=MarketOrder())
    context.open_legs = []


async def _record_greeks(context, data):
    """The book's delta, gamma and theta right now, summed across the open legs."""
    try:
        legs = [await context.option_greeks(listing, amount=amount)
                for listing, amount in context.open_legs]
    except ValueError:
        return  # a leg with no implied volatility this bar
    if not legs:
        return
    from ziplime.finance.options.greeks import aggregate_greeks
    book = aggregate_greeks(legs)
    context.greeks_log.append({
        "dt": context.get_datetime(), "delta": book.delta, "gamma": book.gamma,
        "theta": book.theta, "vega": book.vega,
    })
