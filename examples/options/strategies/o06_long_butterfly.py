"""A long butterfly: "it finishes right about here", paid for up front.

Three strikes on one side -- long a wing, short two of the body, long the other wing. The payoff
is a tent with its peak at the body strike, so the structure is a bet on *where* the underlying
ends rather than on which way it goes, and it is the cheapest way to express that view because the
two short legs pay for most of the two long ones.

**What makes it worth an example of its own.** Every other structure here is two or four legs of
equal size. A butterfly is the first one whose legs are not: the body is short *two*, and that
ratio is the whole structure. Get it wrong and you have a pair of vertical spreads that happen to
share a strike -- which prices similarly, behaves differently, and shows up nowhere in the
transaction list because the fills all look reasonable.

**Entered when the tape is quiet, and that is the view rather than a filter.** A pin bet wants the
underlying to stay where it is; buying one into a trending or volatile tape is paying for a
precision the market is not offering. `realised_vol` is read from the same bar the entry is taken
on, so the decision uses only what had printed by then.

Reported at entry: the debit, the two breakevens, and the maximum profit and loss. They are what
the structure *is*, and a run that does not print them leaves the reader to infer the risk from a
P&L curve.
"""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import butterfly

#: Expiry to trade. Must be one the chain still lists.
EXPIRY = dt.date(2026, 10, 16)
#: Strikes between the body and each wing. One step is the narrowest and most precise tent; wider
#: wings cost more and forgive more.
WING_STEPS = 2
#: Sessions of underlying history the volatility estimate runs over.
VOL_WINDOW = 10
#: Do not buy the tent into a tape moving more than this, annualised. A pin bet into a trend is
#: paying for precision that is not on offer.
MAXIMUM_VOL = 0.45
QUANTITY = 1

WARMUP = VOL_WINDOW


async def initialize(context):
    context.underlying = await context.symbol("AAPL", mic="XNGS")
    context.universe = {"AAPL": context.underlying}
    context.holding = None
    context.done = False


def compute_signals(context, prices):
    """Realised volatility over the whole window, in one pass.

    A trailing standard deviation of log-free simple returns, annualised at 252 sessions. Causal
    by construction -- every value uses only rows at or before its own bar.
    """
    returns = prices.close.pct_change()
    return {"realised_vol": returns.rolling(VOL_WINDOW).std() * (252 ** 0.5)}


async def handle_data(context, data):
    if context.holding is not None or context.done:
        return
    if not context.signals.is_ready("realised_vol"):
        return
    if float(context.signals["realised_vol"]["AAPL"]) > MAXIMUM_VOL:
        return

    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return

    chain = await context.option_chain(context.underlying, expiration_date=EXPIRY)
    calls = sorted(chain.calls, key=lambda listing: listing.asset.strike)
    body = chain.nearest_strike(float(price), OptionType.CALL)
    if body is None:
        return
    strikes = [listing.asset.strike for listing in calls]
    middle = strikes.index(body.asset.strike)
    if middle - WING_STEPS < 0 or middle + WING_STEPS >= len(calls):
        # Not enough strikes either side for the wings asked for. Narrowing them silently would
        # trade a tighter tent than this file describes.
        return
    lower, upper = calls[middle - WING_STEPS], calls[middle + WING_STEPS]

    structure = butterfly(lower=lower, body=body, upper=upper)
    premiums = await _price_legs(data, structure)
    if premiums is None:
        return
    debit = structure.net_premium(premiums)
    context.entry_report = {
        "name": structure.name,
        # `net`, with the library's own sign, so the runner's table can compare structures
        # without knowing which of them is a debit and which a credit.
        "net": debit,
        "breakevens": structure.breakevens(debit),
        "max_profit": structure.max_profit(debit),
        "max_loss": structure.max_loss(debit),
    }

    orders = structure.orders(QUANTITY)
    # The body is short two. A quantity that rounds one leg to zero leaves a vertical spread
    # wearing a butterfly's name, so the shape is checked rather than assumed.
    if len(orders) != 3:
        return
    for listing, amount in orders:
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.holding = orders
    context.done = True


async def _price_legs(data, structure) -> dict[int, float] | None:
    """Premium per unit for every leg, keyed by sid -- what `net_premium` wants.

    One request per leg, and `None` the moment any of them has no market. A structure priced on
    some of its legs is not that structure, and the number it produces looks perfectly plausible.
    """
    prices = {}
    for leg in structure.legs:
        quote = await data.current(assets=[leg.listing], fields=["close"])
        if quote.is_empty() or quote["close"][-1] is None:
            return None
        prices[leg.listing.sid] = float(quote["close"][-1])
    return prices
