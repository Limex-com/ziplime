"""Signals to fills, deterministically.

This is the whole of the kernel, and its job ends where the spec's §4 says it ends: it turns an
investment intention into a fixed sequence of executed transactions. It is **not** a second
portfolio accounting engine. It tracks cash and positions because it cannot answer "does this
order fit" or "is there anything to close" without them, and those numbers come back marked as
execution-time diagnostics. What a run *reports* is built by
:class:`~ziplime.finance.domain.ledger.Ledger` from these fills, and the two disagreeing is a bug
here rather than a difference of opinion.

Four decisions are the kernel's own, taken rather than inherited, and each is pinned by a test:

**Signals are four arrays, not two plus a direction flag.** `entries`/`exits` open and close
longs; `short_entries`/`short_exits` open and close shorts. vectorbt expresses shorts by
overloading `exits` under a `direction` setting, which makes "close the long" and "open a short"
the same array element and leaves a reversal unable to say whether it is one decision or two. Four
arrays say it plainly, and the reversal in §14 falls out as what it is: a close and an open, two
fills, two commissions.

**An exit wins a bar it shares with an entry.** Both true on the same instrument and bar is a
strategy contradicting itself; the kernel does not guess which half was meant. Closing is the half
that cannot lose money it does not have.

**An order that does not fit is rejected whole.** Not silently shrunk -- a half-filled order is a
different strategy, and one nobody asked for. Every rejection is recorded with a reason, because a
strategy that stopped trading because it ran out of money looks exactly like one that stopped
signalling.

**Order within a bar is fixed before anything runs.** Releases before consumptions, then the
instrument order the caller handed in. Anything else makes "who got the last of the cash" depend
on dictionary iteration, which is reproducible within a process and not between them.
"""
from __future__ import annotations

import datetime

import numpy as np
import pandas as pd
import structlog

from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.slippage.slippage_model import SlippageModel

from . import _core, costs
from .models import (
    VECTOR_KERNEL_VERSION, ExecutionTiming, VectorFill, VectorKernelError, VectorReject,
    VectorSimulationResult,
)

_logger = structlog.get_logger(__name__)

#: Phases within one execution bar. Releases first: an exit frees cash that an entry on the same
#: bar may then use, which is the behaviour anyone rebalancing expects and the only one that lets
#: a fully-invested book rotate without a cash buffer.
_CLOSING = ("close_long", "close_short")
_OPENING = ("open_long", "open_short")

#: Intent names to the integers the compiled loop works in, and back. The numbering is the
#: ordering policy: closings below openings, and alphabetical within each phase, so sorting the
#: codes reproduces exactly the order sorting the names produced.
_INTENT_CODES = {
    "close_long": _core.CLOSE_LONG, "close_short": _core.CLOSE_SHORT,
    "open_long": _core.OPEN_LONG, "open_short": _core.OPEN_SHORT,
}
_INTENT_NAMES = {code: name for name, code in _INTENT_CODES.items()}

_REJECT_NAMES = {
    _core.R_SUPERSEDED: "superseded_by_exit",
    _core.R_NAN_PRICE: "nan_price",
    _core.R_NOTHING_TO_CLOSE: "nothing_to_close",
    _core.R_NAN_SIZE: "nan_size",
    _core.R_ZERO_SIZE: "zero_size",
    _core.R_ZERO_SIZE_VOLUME: "zero_size",
    _core.R_INSUFFICIENT_CASH: "insufficient_cash",
}


def _reject_detail(code: int, intent: str, wanted: float) -> str:
    """The sentence that goes with a rejection code.

    Built here rather than inside the loop: a compiled function cannot make a string, and the few
    thousand rejections that reach this are a rounding error against the hundreds of thousands of
    events that do not.
    """
    if code == _core.R_SUPERSEDED:
        return ("an exit for this instrument landed on the same bar; the close is taken and this "
                "open is dropped")
    if code == _core.R_NAN_PRICE:
        return "no usable execution price on this bar"
    if code == _core.R_NOTHING_TO_CLOSE:
        return f"{intent} but the position is {wanted:g}, on the other side"
    if code == _core.R_NAN_SIZE:
        return "the size resolved to NaN"
    if code == _core.R_ZERO_SIZE:
        return "the size resolved to zero units"
    if code == _core.R_ZERO_SIZE_VOLUME:
        return "the bar's volume cap left no room for this order"
    if code == _core.R_INSUFFICIENT_CASH:
        return f"needs {abs(wanted):g} units' worth including fees, and the cash did not cover it"
    return ""


def _as_frame(value, index: pd.DatetimeIndex, columns: list[str], name: str,
              dtype=None) -> pd.DataFrame:
    """Broadcast a scalar, a per-instrument Series or a full frame to T x N. Simple cases only.

    The spec's §7.4 is explicit that this must not grow into vectorbt's broadcasting engine, so
    the three shapes below are the whole of it and anything else is refused by shape rather than
    reinterpreted.
    """
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        missing = set(columns) - set(value.columns)
        if missing:
            raise VectorKernelError(
                f"{name} is missing columns {sorted(missing)}; it has to cover every instrument "
                f"in prices.")
        if not value.index.equals(index):
            raise VectorKernelError(
                f"{name} is indexed differently from prices ({len(value.index)} rows against "
                f"{len(index)}). Align them before calling; the kernel will not reindex, because "
                f"guessing which rows correspond is how a signal ends up on the wrong bar.")
        frame = value[columns]
    elif isinstance(value, pd.Series):
        # Two readings of a Series, told apart by what it is indexed by rather than by a flag.
        # Indexed like the prices it is one value per *bar*, applied to every instrument -- which
        # is the natural shape for a single-instrument run and what vectorbt accepts there.
        # Indexed by the instrument names it is one value per *instrument*, held across all bars.
        # A timestamp index and a ticker index cannot be mistaken for each other, so nothing here
        # is a guess.
        if value.index.equals(index):
            frame = pd.DataFrame(
                np.tile(value.to_numpy().reshape(-1, 1), (1, len(columns))),
                index=index, columns=columns)
        else:
            missing = set(columns) - set(value.index)
            if missing:
                raise VectorKernelError(
                    f"{name} is a Series that is neither indexed like prices (one value per bar) "
                    f"nor by instrument (missing {sorted(missing)}). One of the two, or pass a "
                    f"full frame.")
            frame = pd.DataFrame(
                np.tile(value[columns].to_numpy(), (len(index), 1)), index=index, columns=columns)
    else:
        frame = pd.DataFrame(value, index=index, columns=columns)
    return frame.astype(dtype) if dtype is not None else frame


def _bool_frame(value, index, columns, name) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame(False, index=index, columns=columns)
    frame = _as_frame(value, index, columns, name)
    return frame.fillna(False).astype(bool)


def _normalise_prices(prices, name: str) -> pd.DataFrame:
    if isinstance(prices, pd.Series):
        prices = prices.to_frame(name=prices.name or "asset")
    if not isinstance(prices, pd.DataFrame):
        raise VectorKernelError(f"{name} must be a DataFrame or a Series, got {type(prices)}.")
    if not isinstance(prices.index, pd.DatetimeIndex):
        raise VectorKernelError(f"{name} must be indexed by time, got {type(prices.index)}.")
    if not prices.index.is_monotonic_increasing:
        raise VectorKernelError(
            f"{name} is not in chronological order. The kernel walks it forward and would "
            f"otherwise fill on a bar before the one that signalled.")
    return prices


def simulate_signals(
    prices,
    entries=None,
    exits=None,
    *,
    short_entries=None,
    short_exits=None,
    size=1.0,
    size_type: str = "amount",
    initial_cash: float = 100_000.0,
    execution: ExecutionTiming | None = None,
    opens=None,
    volumes=None,
    commission: CommissionModel | None = None,
    slippage: SlippageModel | None = None,
    cash_sharing: bool = True,
    assets: dict | None = None,
    allow_short: bool = False,
    accumulate: bool = False,
) -> VectorSimulationResult:
    """Run signals through the kernel and return the fills they produced.

    Args:
        prices: Close prices, ``T x N`` indexed by time. A Series is read as one instrument.
        entries: True where a long should be opened or increased.
        exits: True where a long should be closed.
        short_entries: True where a short should be opened. Needs ``allow_short``.
        short_exits: True where a short should be covered.
        size: Units per order -- a scalar, a per-instrument Series, or a ``T x N`` frame.
        size_type: Only ``"amount"`` in this version, i.e. a number of units.
        initial_cash: Starting cash for the single shared pool.
        execution: When a signal becomes a fill. Defaults to next open, which is the honest one;
            pass :meth:`ExecutionTiming.same_close` to match the event engine's
            ``same_bar_execution=True``.
        opens: Open prices, needed only for ``next_open`` timing.
        volumes: Bar volumes, needed only by a slippage model that caps against them.
        commission: A ziplime commission model. Defaults to none.
        slippage: A ziplime slippage model. Defaults to none.
        cash_sharing: One pool across instruments. ``False`` is not implemented.
        assets: ``{column: ExchangeAsset}``, passed to the commission model. Optional; a model
            that ignores its ``asset`` argument -- which the per-share and per-trade ones do --
            does not need it.
        allow_short: Permit short positions. Off by default: a strategy that shorts by accident
            is a strategy nobody tested.
        accumulate: Whether an entry signal on an instrument already held adds to the position.
            Off by default, and that default is a judgement rather than an inheritance. The spec
            reads `entries` as "open **or increase**", which argues for True; what argues louder
            for False is the shape of real signals. `fast > slow` is true for a run of bars, not
            for one, so accumulating turns an ordinary crossover into a position that grows every
            bar the trend holds and exhausts the account -- which is why the repository's own
            sweep example edge-detects its signals before handing them over. Off, a signal array
            means "be in this position"; on, it means "add this much again".

    Returns:
        A :class:`VectorSimulationResult`.

    Raises:
        VectorKernelError: for any shape, option or model the kernel will not guess at.
    """
    prices = _normalise_prices(prices, "prices")
    index, columns = prices.index, list(prices.columns)
    if len(index) == 0:
        raise VectorKernelError("prices has no bars, so there is nothing to simulate.")
    if size_type != "amount":
        raise VectorKernelError(
            f"size_type={size_type!r} is not supported; this version sizes in units of the "
            f"instrument (\"amount\"). Target weights are a later feature -- see the spec's §25.")
    if not cash_sharing:
        raise VectorKernelError(
            "cash_sharing=False is not implemented. Splitting one starting balance into "
            "per-instrument pools has no single obvious rule, and inventing one silently would "
            "decide how much capital each instrument gets without anyone asking for it.")

    timing = execution or ExecutionTiming.next_open()

    # Shapes before prerequisites, and the order is deliberate. A caller who handed in a signal
    # frame missing an instrument, or indexed off the run, has a mistake in the argument they are
    # looking at; telling them instead that `next_open` wants an `opens` frame -- which is true,
    # and about a different argument, and only reachable once the first is fixed -- sends them to
    # the wrong end of their own call.
    entries = _bool_frame(entries, index, columns, "entries")
    exits = _bool_frame(exits, index, columns, "exits")
    short_entries = _bool_frame(short_entries, index, columns, "short_entries")
    short_exits = _bool_frame(short_exits, index, columns, "short_exits")

    if not allow_short and (short_entries.to_numpy().any() or short_exits.to_numpy().any()):
        raise VectorKernelError(
            "short_entries/short_exits were given but allow_short is False. Shorting changes what "
            "a strategy risks, so it is opted into rather than inferred from a signal array.")

    sizes = _as_frame(size, index, columns, "size", dtype=float)
    volumes = (_as_frame(volumes, index, columns, "volumes", dtype=float)
               if volumes is not None else None)
    slip = costs.resolve_slippage(slippage)
    commission_model = costs.resolve_commission(commission)
    assets = assets or {}

    # Last, because it is the only check about a *prerequisite* rather than about an argument the
    # caller already passed: everything above can be wrong on its own terms.
    opens = _normalise_prices(opens, "opens") if opens is not None else None
    if timing.price_field == "open":
        if opens is None:
            raise VectorKernelError(
                "execute_at='next_open' needs open prices, and none were given. Pass "
                "opens=<DataFrame of opens>, or use next_close/same_close, which fill at the "
                "closes already in `prices`.")
        missing = set(columns) - set(opens.columns)
        if missing:
            raise VectorKernelError(f"opens is missing {sorted(missing)}.")
        if not opens.index.equals(index):
            raise VectorKernelError("opens is indexed differently from prices.")

    execution_prices = (opens if timing.price_field == "open" else prices)[columns]
    close_values = prices[columns].to_numpy(dtype=float)
    execution_values = execution_prices.to_numpy(dtype=float)
    size_values = sizes.to_numpy(dtype=float)
    volume_values = volumes.to_numpy(dtype=float) if volumes is not None else None
    signal_arrays = {
        "open_long": entries.to_numpy(), "close_long": exits.to_numpy(),
        "open_short": short_entries.to_numpy(), "close_short": short_exits.to_numpy(),
    }

    n_bars, n_columns = len(index), len(columns)

    # Events, as arrays rather than as tuples. Only the cells that carry a signal: walking the
    # whole T x N grid in Python was the kernel's entire cost at scale -- 500 instruments over
    # five years is 2.5 million cells before a single order exists -- and the signals are sparse.
    bar_offset = timing.bar_offset
    event_bars, event_cols, event_intents, event_signal_bars = [], [], [], []
    late_rejects: list[VectorReject] = []
    for intent, array in signal_arrays.items():
        code = _INTENT_CODES[intent]
        signal_bars, column_ixs = np.nonzero(array)
        execution_bars = signal_bars + bar_offset
        past_end = execution_bars >= n_bars
        if past_end.any():
            for signal_bar, column_ix in zip(signal_bars[past_end].tolist(),
                                             column_ixs[past_end].tolist()):
                late_rejects.append(VectorReject(
                    instrument=columns[column_ix], timestamp=index[signal_bar], intent=intent,
                    reason="no_execution_bar", wanted_amount=0.0,
                    signal_timestamp=index[signal_bar],
                    detail="the signal fell on the last bar; there is no later bar to fill on"))
        keep = ~past_end
        event_bars.append(execution_bars[keep])
        event_cols.append(column_ixs[keep])
        event_intents.append(np.full(int(keep.sum()), code, dtype=np.int64))
        event_signal_bars.append(signal_bars[keep])

    event_bar = np.concatenate(event_bars).astype(np.int64)
    event_col = np.concatenate(event_cols).astype(np.int64)
    event_intent = np.concatenate(event_intents)
    event_signal_bar = np.concatenate(event_signal_bars).astype(np.int64)

    # One sort for the whole run, on the key the ordering policy is defined by: execution bar,
    # then closings before openings, then the caller's column order, then the intent. The intent
    # codes are numbered so that this reproduces the order the string keys sorted by.
    #
    # Packed into a single integer rather than handed to `np.lexsort`, which runs one stable pass
    # per key -- four passes over six hundred thousand events, and it was the largest cost left
    # outside the compiled loop. The four fields are disjoint bit ranges of an int64, so ordering
    # the packed number orders the tuple. The widths are generous: the product below stays under
    # 2^63 for any run that fits in memory.
    phase = (event_intent >= _core.OPEN_LONG).astype(np.int64)
    sort_key = ((event_bar * 2 + phase) * n_columns + event_col) * 4 + event_intent
    order = np.argsort(sort_key, kind="stable")
    event_bar = np.ascontiguousarray(event_bar[order])
    event_col = np.ascontiguousarray(event_col[order])
    event_intent = np.ascontiguousarray(event_intent[order])
    event_signal_bar = np.ascontiguousarray(event_signal_bar[order])

    # Which (bar, instrument) cells carry a close, per direction. Per direction is the whole of
    # it: a close-long sharing a bar with an open-*short* is a reversal, which a single "is there
    # a close here" mask silently swallowed.
    closing_long = np.zeros((n_bars, n_columns), dtype=np.bool_)
    closing_short = np.zeros((n_bars, n_columns), dtype=np.bool_)
    is_close_long = event_intent == _core.CLOSE_LONG
    is_close_short = event_intent == _core.CLOSE_SHORT
    closing_long[event_bar[is_close_long], event_col[is_close_long]] = True
    closing_short[event_bar[is_close_short], event_col[is_close_short]] = True

    volume_array = (np.ascontiguousarray(volume_values, dtype=np.float64)
                    if volume_values is not None
                    else np.zeros((1, 1), dtype=np.float64))

    (fill_ix, fill_amount, fill_price, fill_fee, n_fills,
     reject_ix, reject_code, reject_wanted, n_rejects,
     no_ops, cash_series, positions) = _core.run_events(
        event_bar, event_col, event_intent, event_signal_bar,
        np.ascontiguousarray(execution_values, dtype=np.float64),
        np.ascontiguousarray(close_values, dtype=np.float64),
        np.ascontiguousarray(size_values, dtype=np.float64),
        volume_array, volume_values is not None,
        closing_long, closing_short,
        float(slip.percentage), float(slip.spread),
        float(slip.volume_limit or 0.0), slip.volume_limit is not None,
        float(commission_model.per_unit), float(commission_model.per_dollar),
        float(commission_model.fixed), float(commission_model.min_trade),
        float(initial_cash), 1 if accumulate else 0, n_bars, n_columns,
    )

    # Names, timestamps and objects are rebuilt only for the events that produced something --
    # a few thousand out of a few hundred thousand.
    stamps = [stamp.to_pydatetime() if hasattr(stamp, "to_pydatetime") else stamp
              for stamp in index]
    fills = [
        VectorFill(
            instrument=columns[event_col[ix]], timestamp=stamps[event_bar[ix]],
            amount=float(fill_amount[position]), price=float(fill_price[position]),
            commission=float(fill_fee[position]), sequence=position,
            intent=_INTENT_NAMES[event_intent[ix]],
            signal_timestamp=stamps[event_signal_bar[ix]])
        for position, ix in enumerate(fill_ix[:n_fills].tolist())
    ]
    rejects = list(late_rejects)
    rejects.extend(
        VectorReject(
            instrument=columns[event_col[ix]], timestamp=stamps[event_bar[ix]],
            intent=_INTENT_NAMES[event_intent[ix]],
            reason=_REJECT_NAMES[reject_code[position]],
            wanted_amount=float(reject_wanted[position]),
            signal_timestamp=stamps[event_signal_bar[ix]],
            detail=_reject_detail(int(reject_code[position]),
                                  _INTENT_NAMES[event_intent[ix]],
                                  float(reject_wanted[position])))
        for position, ix in enumerate(reject_ix[:n_rejects].tolist())
    )

    # Marking the book is one matrix multiply at the end rather than a dot product per bar. NaN
    # closes contribute nothing rather than poisoning the row -- a price the bundle does not carry
    # is a gap, and a gap is not a position worth zero.
    marks = np.where(np.isfinite(close_values), close_values, 0.0)
    value_series = cash_series + np.einsum("ij,ij->i", positions, marks)

    _logger.info("Vector kernel simulated signals", bars=n_bars, instruments=n_columns,
                 fills=len(fills), rejects=len(rejects), no_ops=int(no_ops),
                 timing=timing.execute_at, compiled=_core.HAVE_NUMBA,
                 version=VECTOR_KERNEL_VERSION)
    return VectorSimulationResult(
        index=index, fills=fills, rejects=rejects, instruments=columns,
        cash=pd.Series(cash_series, index=index, name="cash"),
        portfolio_value=pd.Series(value_series, index=index, name="portfolio_value"),
        positions=pd.DataFrame(positions, index=index, columns=columns),
        initial_cash=float(initial_cash), timing=timing, no_ops=int(no_ops))


def _wanted_amount(intent: str, position: float, size: float) -> float | None:
    """The signed amount an intent asks for, or None when there is nothing to act on.

    The compiled loop has this inline -- it cannot call out -- so this is no longer on the hot
    path. It is kept because it is the seam a future `simulate_weights` replaces (the spec's
    §25): everything else in the kernel is about *executing* an amount, and this is the single
    place an intent becomes one. `tests/test_vector_kernel.py` holds it to the same answers the
    compiled version gives.
    """
    if intent == "open_long":
        return abs(size)
    if intent == "open_short":
        return -abs(size)
    if intent == "close_long":
        return -position if position > 0 else None
    if intent == "close_short":
        return -position if position < 0 else None
    raise VectorKernelError(f"unknown intent {intent!r}")
