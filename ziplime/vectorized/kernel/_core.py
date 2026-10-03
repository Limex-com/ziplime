"""The inner loop, written so that Numba can compile it -- and so that it is still correct without.

There is **one** implementation here, not two. The body below is plain loops over NumPy arrays
with no Python objects in sight, which is both what `numba.njit` requires and what ordinary
CPython runs perfectly well. When Numba is installed the decorator compiles it; when it is not,
the decorator is a no-op and the same function runs interpreted. A second, "readable" version
kept alongside a fast one is how the two quietly stop agreeing, so there isn't one.

What that costs: everything the loop needs has to arrive as a number or an array. Slippage and
commission are therefore reduced to parameters by :mod:`ziplime.vectorized.kernel.costs` before
the call, intents are integers rather than strings, and rejections come back as codes. The
translation back into names happens in :mod:`ziplime.vectorized.kernel.simulation`, on the few
hundred events that produced something, rather than on the few hundred thousand that did not.

Compilation is not free either -- the first call pays a few hundred milliseconds to compile -- so
:func:`ziplime.vectorized.kernel.simulation.simulate_signals` warms it once rather than paying it
inside a parameter sweep's first combination and reporting that as the kernel's speed.
"""
from __future__ import annotations

import numpy as np

#: Intents, as integers. The order matters and is not arbitrary: closings sort before openings,
#: and within a phase the numbering reproduces the alphabetical order the string version sorted
#: by, so the execution order is the one the tests already pinned.
CLOSE_LONG = 0
CLOSE_SHORT = 1
OPEN_LONG = 2
OPEN_SHORT = 3

#: Rejection codes. Mapped back to names in `simulation._REJECT_NAMES`.
R_SUPERSEDED = 0
R_NAN_PRICE = 1
R_NOTHING_TO_CLOSE = 2
R_NAN_SIZE = 3
R_ZERO_SIZE = 4
R_ZERO_SIZE_VOLUME = 5
R_INSUFFICIENT_CASH = 6

try:  # pragma: no cover - depends on the install, and both branches are exercised by tests
    from numba import njit

    HAVE_NUMBA = True
except ImportError:  # pragma: no cover
    HAVE_NUMBA = False

    def njit(*args, **kwargs):
        """Stand-in for Numba's decorator: returns the function unchanged.

        The kernel stays correct and dependency-free without Numba; it is simply slower. That is
        a performance characteristic rather than a behaviour change, which is why this is a
        fallback and not a refusal.
        """
        if args and callable(args[0]):
            return args[0]

        def wrap(function):
            return function

        return wrap


@njit(cache=True, nogil=True)
def run_events(
    event_bar, event_col, event_intent, event_signal_bar,
    execution_values, close_values, size_values, volume_values, has_volume,
    closing_long, closing_short,
    slip_percentage, slip_spread, slip_volume_limit, has_volume_limit,
    com_per_unit, com_per_dollar, com_fixed, com_min_trade,
    initial_cash, accumulate, n_bars, n_columns,
):
    """Walk the pre-sorted events, decide every fill, and report what happened.

    Every decision the Python version took is taken here, in the same order. The arrays come back
    sized to the number of events and sliced by the counts returned, because a compiled function
    cannot append to a list.

    Returns:
        A tuple of ``(fill_ix, fill_amount, fill_price, fill_fee, n_fills, reject_ix,
        reject_code, reject_wanted, n_rejects, no_ops, cash_series, positions)``, where the ``ix``
        arrays index back into the events the caller passed.
    """
    n_events = event_bar.shape[0]

    fill_ix = np.empty(n_events, dtype=np.int64)
    fill_amount = np.empty(n_events, dtype=np.float64)
    fill_price = np.empty(n_events, dtype=np.float64)
    fill_fee = np.empty(n_events, dtype=np.float64)
    reject_ix = np.empty(n_events, dtype=np.int64)
    reject_code = np.empty(n_events, dtype=np.int64)
    reject_wanted = np.empty(n_events, dtype=np.float64)

    cash_series = np.empty(n_bars, dtype=np.float64)
    positions = np.zeros((n_bars, n_columns), dtype=np.float64)
    held = np.zeros(n_columns, dtype=np.float64)

    cash = initial_cash
    n_fills = 0
    n_rejects = 0
    no_ops = 0
    event_ix = 0

    for bar in range(n_bars):
        while event_ix < n_events and event_bar[event_ix] == bar:
            column = event_col[event_ix]
            intent = event_intent[event_ix]
            this_event = event_ix
            event_ix += 1

            position = held[column]

            # The no-op tests come first, before any price is touched. With level signals these
            # are the overwhelming majority of events, and an intent that is already satisfied is
            # a no-op even on a bar whose price is missing -- reporting `nan_price` there would be
            # reporting a problem with a fill that was never going to happen.
            if accumulate == 0:
                if intent == OPEN_LONG and position > 0.0:
                    no_ops += 1
                    continue
                if intent == OPEN_SHORT and position < 0.0:
                    no_ops += 1
                    continue
            if position == 0.0 and (intent == CLOSE_LONG or intent == CLOSE_SHORT):
                no_ops += 1
                continue

            # An exit and an entry facing the same way, on the same instrument and bar, is the
            # strategy contradicting itself. The close is taken and the open dropped. A close
            # facing the *other* way is a reversal and must not be caught here.
            if intent == OPEN_LONG and closing_long[bar, column]:
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_SUPERSEDED
                reject_wanted[n_rejects] = 0.0
                n_rejects += 1
                continue
            if intent == OPEN_SHORT and closing_short[bar, column]:
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_SUPERSEDED
                reject_wanted[n_rejects] = 0.0
                n_rejects += 1
                continue

            price = execution_values[bar, column]
            if not np.isfinite(price):
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_NAN_PRICE
                reject_wanted[n_rejects] = 0.0
                n_rejects += 1
                continue

            size = size_values[bar, column]
            if intent == OPEN_LONG:
                wanted = abs(size)
            elif intent == OPEN_SHORT:
                wanted = -abs(size)
            elif intent == CLOSE_LONG:
                if position <= 0.0:
                    reject_ix[n_rejects] = this_event
                    reject_code[n_rejects] = R_NOTHING_TO_CLOSE
                    reject_wanted[n_rejects] = position
                    n_rejects += 1
                    continue
                wanted = -position
            else:
                if position >= 0.0:
                    reject_ix[n_rejects] = this_event
                    reject_code[n_rejects] = R_NOTHING_TO_CLOSE
                    reject_wanted[n_rejects] = position
                    n_rejects += 1
                    continue
                wanted = -position

            if not np.isfinite(wanted):
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_NAN_SIZE
                reject_wanted[n_rejects] = 0.0
                n_rejects += 1
                continue
            if wanted == 0.0:
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_ZERO_SIZE
                reject_wanted[n_rejects] = 0.0
                n_rejects += 1
                continue

            direction = 1.0 if wanted > 0.0 else -1.0
            filled_price = price + price * slip_percentage * direction + \
                slip_spread / 2.0 * direction

            capped = abs(wanted)
            if has_volume_limit and has_volume:
                room = volume_values[bar, column] * slip_volume_limit
                if room < 0.0:
                    room = 0.0
                if room < capped:
                    capped = room
            if capped <= 0.0:
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_ZERO_SIZE_VOLUME
                reject_wanted[n_rejects] = wanted
                n_rejects += 1
                continue

            amount = direction * capped
            value = amount * filled_price
            fee = com_per_unit * abs(amount) + com_per_dollar * abs(value) + com_fixed
            if fee < com_min_trade:
                fee = com_min_trade

            cost = value + fee          # positive when buying, negative when selling
            if cost > 0.0 and cost > cash + 1e-9:
                reject_ix[n_rejects] = this_event
                reject_code[n_rejects] = R_INSUFFICIENT_CASH
                reject_wanted[n_rejects] = amount
                n_rejects += 1
                continue

            cash -= cost
            held[column] = position + amount
            fill_ix[n_fills] = this_event
            fill_amount[n_fills] = amount
            fill_price[n_fills] = filled_price
            fill_fee[n_fills] = fee
            n_fills += 1

        for column in range(n_columns):
            positions[bar, column] = held[column]
        cash_series[bar] = cash

    return (fill_ix, fill_amount, fill_price, fill_fee, n_fills,
            reject_ix, reject_code, reject_wanted, n_rejects,
            no_ops, cash_series, positions)


def warm_up() -> bool:
    """Compile the kernel on a trivial input, so a sweep's first combination is not the one that
    pays for it. Returns whether Numba is actually in play."""
    if not HAVE_NUMBA:
        return False
    empty_i = np.zeros(1, dtype=np.int64)
    empty_f = np.zeros((2, 1), dtype=np.float64)
    empty_b = np.zeros((2, 1), dtype=np.bool_)
    run_events(empty_i, empty_i, empty_i, empty_i, empty_f, empty_f, empty_f, empty_f, False,
               empty_b, empty_b, 0.0, 0.0, 0.0, False, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 2, 1)
    return True
