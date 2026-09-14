"""The twin of ``o03_vectorised_0dte_straddle.py``, written the ordinary bar-by-bar way.

Same strategy, same constants, same decisions -- and the two signals recomputed from a fresh
history window on every minute instead of once before the first bar. This is the version the
vectorised one has to agree with, and the cost it has to beat: at every minute of every session it
re-reads up to a session's worth of closes and re-derives a standard deviation that one pass over
the array already knows for every bar.

**Why this file exists at all.** Options never go through the vector *kernel* -- that path refuses
them, because a contract is worth ``price x multiplier x amount`` and settles at expiry, neither
of which a fills-and-cash kernel models. So for an option strategy "vectorised" can only ever mean
``compute_signals``, and the only way to know that the arithmetic moved without the strategy
changing is to write it both ways and require the same orders. ``run_0dte_straddle.py --compare``
runs both and diffs the fills.

**The window arithmetic is the whole risk here.** ``data.history`` stops *before* the current bar,
while the signal panel's row **is** the current bar -- the same convention ``data.current`` uses.
So a window that the vectorised side expresses as "this session's bars so far" is
``history(n - 1)`` plus ``current`` here. Getting it wrong by one does not raise; it quietly
computes a slightly different strategy, which is precisely what the comparison is for.
"""
import datetime as dt

from ziplime.assets.domain.option_type import OptionType
from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import straddle

# Deliberately the same numbers as the vectorised twin. If these ever drift apart the comparison
# stops meaning anything, which is why they are written out rather than imported -- an import
# would hide a divergence behind a name that still resolved.
ENTRY_MINUTE = 30
EXIT_MINUTE = 30
VOL_WINDOW = 30
MOVE_TARGET = 0.004
MINIMUM_VOL = 0.005
QUANTITY = 1

#: Minutes in a regular session, the scale the per-minute standard deviation is lifted to.
SESSION_MINUTES = 390


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")
    context.holding = None
    context.done_for = None


async def handle_data(context, data):
    now = context.get_datetime()
    session = now.date()
    minutes_in = _minutes_since_open(context, now)
    minutes_left = _minutes_to_close(context, now)

    if context.holding is None:
        if context.done_for == session:
            return
        if minutes_in < ENTRY_MINUTE or minutes_left <= EXIT_MINUTE:
            return
        closes = await _session_closes(context, data, minutes_in)
        volatility = _realised_vol(closes)
        if volatility is None or volatility < MINIMUM_VOL:
            return
        await _open_straddle(context, data, session)
        return

    closes = await _session_closes(context, data, minutes_in)
    moved = abs(closes[-1] / closes[0] - 1.0) if len(closes) > 1 else 0.0
    if moved >= MOVE_TARGET or minutes_left <= EXIT_MINUTE:
        for listing, amount in context.holding:
            await context.order(asset=listing, amount=-amount, style=MarketOrder())
        context.holding = None
        context.done_for = session


async def _session_closes(context, data, minutes_in: float) -> list[float]:
    """This session's closes up to and including the current bar.

    `session_first_minute` **is** the session's first clock bar -- measured, not assumed -- so
    `minutes_in` is the current bar's *index* within the session, and the number of bars so far is
    one more than that. `history` is then asked for one fewer again, because it stops before the
    current bar and `current` supplies the last one.

    The first version of this took `minutes_in` as the count, on the belief that the clock started
    a minute after the calendar's open. It does not. The window came out one bar short, realised
    volatility crossed its threshold a bar late, and the two engines entered on different minutes
    -- which is exactly the kind of silent divergence the comparison exists to surface.
    """
    bars = max(int(round(minutes_in)) + 1, 1)
    past = []
    if bars > 1:
        window = await data.history(assets=[context.underlying], bar_count=bars - 1,
                                    fields=["close"], frequency=dt.timedelta(minutes=1))
        past = [value for value in window["close"].to_list() if value is not None]
    current = await data.current(assets=[context.underlying], fields=["close"])
    latest = current["close"][0]
    if latest is None:
        return past
    return [*past, float(latest)]


def _realised_vol(closes: list[float]) -> float | None:
    """The vectorised side's `returns.rolling(VOL_WINDOW).std() * sqrt(390)`, at this bar.

    `None` where the rolling window is not yet full, which is what `is_ready` reports there. The
    returns are intra-session only: the vectorised twin masks the overnight one, so a window that
    would reach across a session boundary produces nothing on both sides rather than a spike on
    one of them.
    """
    if len(closes) < VOL_WINDOW + 1:
        return None
    window = closes[-(VOL_WINDOW + 1):]
    returns = [window[i] / window[i - 1] - 1.0 for i in range(1, len(window))]
    mean = sum(returns) / len(returns)
    # Sample standard deviation, n-1, which is pandas' default and therefore the twin's.
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    return (variance ** 0.5) * (SESSION_MINUTES ** 0.5)


async def _open_straddle(context, data, session: dt.date):
    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return

    chain = await context.option_chain(context.underlying, expiration_date=session)
    call = chain.nearest_strike(float(price), OptionType.CALL)
    if call is None:
        return
    # The put has to share the call's strike; a different one would silently make this a strangle.
    put = chain.at_strike(call.asset.strike, OptionType.PUT)
    if put is None:
        return

    quotes = await data.current(assets=[call, put], fields=["close"])
    if any(value is None for value in quotes["close"].to_list()):
        return

    orders = straddle(call_listing=call, put_listing=put).orders(QUANTITY)
    for listing, amount in orders:
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.holding = orders


def _minutes_since_open(context, now) -> float:
    calendar = context.clock.trading_calendar
    opened = calendar.session_first_minute(now.date()).tz_convert(now.tzinfo)
    return (now - opened.to_pydatetime()).total_seconds() / 60.0


def _minutes_to_close(context, now) -> float:
    calendar = context.clock.trading_calendar
    closes = calendar.session_close(now.date()).tz_convert(now.tzinfo)
    return (closes.to_pydatetime() - now).total_seconds() / 60.0
