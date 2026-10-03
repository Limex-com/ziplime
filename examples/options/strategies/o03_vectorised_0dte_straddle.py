"""Buy the at-the-money 0DTE straddle, with the volatility arithmetic vectorised.

A long straddle is a bet that the underlying moves, in either direction, by more than the two
premiums cost. On a 0DTE contract that bet is settled by the close of the same session: whatever
has not happened by 16:00 does not happen, and both legs decay toward intrinsic value all day.
So the strategy is entirely about *when* to be in it.

**What is vectorised, and why that split.** Realised volatility over a trailing window and the
move away from the session's open are trailing-window arithmetic over the whole minute grid --
computed once, before the first bar, by ``compute_signals``. What stays in ``handle_data`` is
everything that reads the book: which contracts the chain actually holds, whether a position is
already open, and how close the clock is to the close. That is the rule the hybrid rests on --
signals vectorise, decisions do not -- and options make it unavoidable rather than merely tidy:
the vector *kernel* refuses options outright, because a contract is worth ``price x multiplier x
amount`` and is settled at expiry, neither of which a fills-and-cash kernel models. Here the
ledger does all of that and only the arithmetic moves.

**The entry is deliberately not at the open.** The first minutes of a 0DTE session are where the
spread is widest and the premium richest, and a straddle bought there pays for both. Waiting for
``ENTRY_MINUTE`` is a decision, not a detail, and moving it changes the strategy more than any
other constant in this file.

**The exit is the part that matters.** A 0DTE straddle held to the close is worth its intrinsic
value, which for an at-the-money strike is usually near zero -- so the position is closed at
``EXIT_MINUTE`` regardless, and earlier if the underlying has already moved enough to pay for it.
Holding to expiry is not a strategy; it is the absence of one.
"""
import datetime as dt

import pandas as pd

from ziplime.finance.execution import MarketOrder
from ziplime.finance.options.strategies import straddle

#: Minutes after the open before the straddle is bought.
ENTRY_MINUTE = 30
#: Minutes before the close when the position is closed, whatever it is worth.
EXIT_MINUTE = 30
#: Trailing window for realised volatility, in minutes.
VOL_WINDOW = 30
#: Close early once the underlying has moved this far from the session's open, in either
#: direction. A straddle that has already been paid for is a straddle worth taking off.
#:
#: Over the sample window nine of nineteen sessions reached it, so both exits -- this one and the
#: clock -- are exercised rather than one of them being dead code that nobody notices.
MOVE_TARGET = 0.004
#: Do not open into a tape quieter than this, measured as the trailing realised volatility scaled
#: to a session. A straddle needs movement to pay for itself.
#:
#: Read off the data rather than guessed, and the first guess is why that is worth saying: 0.04
#: was six times the largest value SPY produced over the sample window, so it filtered out every
#: session and the backtest reported zero trades and a perfectly flat curve -- which looks like a
#: strategy that does nothing rather than a threshold that stops everything. Measured over
#: 2026-08-17..2026-09-11, realised volatility at the entry minute ran 0.0039 to 0.0103 with a
#: median of 0.0068, so this sits just below the median and skips roughly the quietest quarter.
#:
#: It is therefore calibrated to *this* window and is a demonstration, not a tuned parameter. On
#: any other period it has to be re-derived.
MINIMUM_VOL = 0.005
#: Straddles to buy. One is 100 shares of exposure per leg.
QUANTITY = 1

#: No warm-up: a 0DTE session is the whole of this strategy's history, and reaching back into the
#: previous one would be reaching into a chain that no longer exists.
WARMUP = 0


async def initialize(context):
    context.underlying = await context.symbol("SPY", mic="ARCX")
    #: `compute_signals` computes over this, and the keys become the panel's column names.
    context.universe = {"SPY": context.underlying}
    context.holding = None
    #: The session this strategy has already had its one straddle in. Keyed by date rather than
    #: a bare flag so the same file works over many sessions.
    context.done_for = None


def compute_signals(context, prices):
    """One pass over every minute in the window, before the first bar.

    **Both signals are per session, and that is the whole subtlety.** `close.iloc[0]` is the
    *panel's* first row, not the session's -- and on a single-session run those are the same row,
    which is exactly why the mistake survives testing. Over nineteen sessions it does not: SPY
    opened the window near 775 and was near 764 by the end, so a move measured from the panel's
    first bar was permanently past any intraday target, and the straddle was sold one minute after
    it was bought on every session but the first. The P&L looked plausible -- a few dollars a day
    -- because a one-minute hold loses about the spread.

    Grouping by the index's date fixes it, and stays causal: `verify_causality` truncates with
    `iloc[:rows]`, so a session's first bar is always present in the truncated panel too, and the
    value at any given bar does not change when the future is removed.
    """
    close = prices.close
    sessions = close.index.date
    session_open = close.groupby(sessions).transform("first")
    # Returns within a session only. Across the overnight gap `pct_change` would measure the gap
    # itself, which is not intraday volatility and would put a spike at every session's open.
    returns = close.pct_change().where(pd.Series(sessions, index=close.index).eq(
        pd.Series(sessions, index=close.index).shift()), other=float("nan"))
    return {
        # Annualising is pointless intraday and would only obscure the number; this is the
        # per-minute standard deviation scaled to a session.
        "realised_vol": returns.rolling(VOL_WINDOW).std() * (390 ** 0.5),
        "move_from_open": (close / session_open - 1.0).abs(),
    }


async def handle_data(context, data):
    now = context.get_datetime()
    session = now.date()
    minutes_in = _minutes_since_open(context, now)
    minutes_left = _minutes_to_close(context, now)

    if context.holding is None:
        # One straddle per session. Without this the strategy re-enters on the bar after it takes
        # profit -- `move_from_open` does not fall back below the target once it has been crossed,
        # so the next bar exits again, and the session becomes a churn loop. Measured on
        # 2026-09-11 at a 0.15% target: 252 transactions instead of 4. It went unnoticed at a
        # 0.4% target only because that threshold was never crossed, which is luck rather than
        # correctness.
        if context.done_for == session:
            return
        if minutes_in < ENTRY_MINUTE or minutes_left <= EXIT_MINUTE:
            return
        # The second signal earns its place here. A straddle bought into a dead tape pays theta
        # for a move that is not coming; this is the cheapest possible statement of that, and it
        # is the reason `realised_vol` is computed rather than merely available.
        if not context.signals.is_ready("realised_vol"):
            return
        if float(context.signals["realised_vol"]["SPY"]) < MINIMUM_VOL:
            return
        await _open_straddle(context, data, session)
        return

    # Already in it. Two ways out, and the clock is the one that always fires.
    moved = float(context.signals["move_from_open"]["SPY"])
    if moved >= MOVE_TARGET or minutes_left <= EXIT_MINUTE:
        for listing, amount in context.holding:
            await context.order(asset=listing, amount=-amount, style=MarketOrder())
        context.holding = None
        context.done_for = session


async def _open_straddle(context, data, session: dt.date):
    """Buy the at-the-money straddle in today's chain, if both legs quote."""
    spot = await data.current(assets=[context.underlying], fields=["close"])
    price = spot["close"][0]
    if price is None:
        return

    chain = await context.option_chain(context.underlying, expiration_date=session)
    from ziplime.assets.domain.option_type import OptionType

    call = chain.nearest_strike(float(price), OptionType.CALL)
    put = chain.at_strike(_strike_of(call), OptionType.PUT)
    if call is None or put is None:
        # A chain with a call and no put at that strike is not a straddle, and substituting a
        # different strike would silently make it a strangle.
        return

    quotes = await data.current(assets=[call, put], fields=["close"])
    if any(value is None for value in quotes["close"].to_list()):
        return

    structure = straddle(call_listing=call, put_listing=put)
    orders = structure.orders(QUANTITY)
    for listing, amount in orders:
        await context.order(asset=listing, amount=amount, style=MarketOrder())
    context.holding = orders


def _strike_of(listing) -> float:
    return listing.asset.strike


def _minutes_since_open(context, now) -> float:
    # The calendar lives on the clock; a strategy has no separate handle on it.
    calendar = context.clock.trading_calendar
    opened = calendar.session_first_minute(now.date()).tz_convert(now.tzinfo)
    return (now - opened.to_pydatetime()).total_seconds() / 60.0


def _minutes_to_close(context, now) -> float:
    calendar = context.clock.trading_calendar
    closes = calendar.session_close(now.date()).tz_convert(now.tzinfo)
    return (closes.to_pydatetime() - now).total_seconds() / 60.0
