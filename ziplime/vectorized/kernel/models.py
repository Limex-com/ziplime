"""The kernel's own vocabulary: what goes in, what comes out, and what it refuses to guess.

Defined before the simulation that produces them, because the contract is the part that has to
outlive the implementation. The compatibility wrapper at the bottom is the exception -- it exists
to fit an interface that was shaped by another library, and it is meant to be deleted.
"""
from __future__ import annotations

import dataclasses
import datetime
from typing import Literal

import pandas as pd

#: Bumped when the kernel's *decisions* change -- ordering, rejection, cost application -- not
#: when its implementation is tidied. A result carries it so a number can be traced back to the
#: semantics that produced it.
VECTOR_KERNEL_VERSION = "1.0.0"

#: Which price a fill is taken at, given the bar the signal was raised on.
#:
#: `same_close` is the look-ahead one: the decision is taken on a bar's close and filled at that
#: same close, a price the market had not printed when the decision was made. It is offered
#: because the event-driven engine offers it (`same_bar_execution`) and the two paths have to be
#: able to express the same run -- not because it is realistic.
TimingMode = Literal["same_close", "next_open", "next_close"]

#: Why an intent produced no fill. Every rejection carries one; none of them is silent.
RejectReason = Literal[
    "nan_price",           # the execution bar has no usable price
    "no_execution_bar",    # the signal fell on the last bar and execution would be past the end
    "insufficient_cash",   # fill-or-reject: the whole order did not fit
    "nothing_to_close",    # an exit with no position behind it
    "zero_size",           # the size resolved to zero shares, or a volume cap left no room
    "nan_size",            # the size itself was not a number
    "superseded_by_exit",  # an entry that shared its bar with an exit for the same instrument
]


class VectorKernelError(ValueError):
    """Anything the kernel refuses to do. Never a silent fallback -- see the spec's §36."""


@dataclasses.dataclass(frozen=True)
class ExecutionTiming:
    """When a signal becomes a fill.

    The kernel takes this explicitly rather than expecting the caller to have shifted its own
    arrays. A `.shift()` buried in strategy code is a look-ahead decision written in a place
    nobody reviews as one, and it cannot be compared against the event-driven engine's
    `same_bar_execution` because the two are not written down in the same terms.

    Attributes:
        signal_at: Which price the *decision* is taken on. Only "close" in this version --
            a signal read off an intraday high is a different feature, not a timing mode.
        execute_at: Which price the fill is taken at: the same bar's close, the next bar's open,
            or the next bar's close.
    """

    signal_at: Literal["close"] = "close"
    execute_at: TimingMode = "next_open"

    def __post_init__(self):
        if self.signal_at != "close":
            raise VectorKernelError(
                f"signal_at={self.signal_at!r} is not supported; this version reads signals off "
                f"the close only.")
        if self.execute_at not in ("same_close", "next_open", "next_close"):
            raise VectorKernelError(
                f"execute_at={self.execute_at!r} is not a timing mode. Use one of: "
                f"same_close, next_open, next_close.")

    @property
    def bar_offset(self) -> int:
        """How many bars after the signal the fill lands."""
        return 0 if self.execute_at == "same_close" else 1

    @property
    def price_field(self) -> str:
        """Which price frame the fill is taken from."""
        return "open" if self.execute_at == "next_open" else "close"

    @property
    def is_look_ahead(self) -> bool:
        """True when the fill price had not printed when the decision was taken."""
        return self.execute_at == "same_close"

    # Named constructors, so a caller reads as a sentence rather than as two keyword arguments.
    @classmethod
    def same_close(cls) -> "ExecutionTiming":
        return cls(execute_at="same_close")

    @classmethod
    def next_open(cls) -> "ExecutionTiming":
        return cls(execute_at="next_open")

    @classmethod
    def next_close(cls) -> "ExecutionTiming":
        return cls(execute_at="next_close")


@dataclasses.dataclass(frozen=True)
class VectorFill:
    """One executed transaction.

    `amount` is signed -- positive buys, negative sells -- which is ziplime's convention and not
    vectorbt's magnitude-plus-side. The side is derived on the way out to the compatibility frame,
    never carried as a second source of truth about direction.
    """

    instrument: str
    timestamp: datetime.datetime
    amount: float
    price: float
    commission: float
    #: Order within the whole run, so a bar holding several fills replays in a fixed order.
    sequence: int
    #: What the fill was for: open_long, close_long, open_short, close_short.
    intent: str
    #: The bar the decision was taken on, which `timestamp` is the execution of.
    signal_timestamp: datetime.datetime

    @property
    def side(self) -> str:
        return "Buy" if self.amount > 0 else "Sell"

    @property
    def value(self) -> float:
        """Signed cash effect before commission: negative when buying."""
        return -self.amount * self.price


@dataclasses.dataclass(frozen=True)
class VectorReject:
    """An intent that could have filled and did not, and why.

    Kept rather than dropped because the difference between "the strategy did not signal" and
    "the strategy signalled and could not be filled" is invisible in a fills table, and it is the
    difference between a strategy that does nothing and one that is out of money.

    An entry on an instrument already held is **not** one of these. It could not have filled --
    the position it asks for is already open -- so it is a no-op rather than a refusal, counted in
    :attr:`VectorSimulationResult.no_ops` and not recorded here. The distinction is not pedantry:
    a level signal like `fast > slow` is true for a run of bars, so recording each repeat buried
    the real refusals under about seventy of them apiece.
    """

    instrument: str
    timestamp: datetime.datetime
    intent: str
    reason: RejectReason
    wanted_amount: float
    signal_timestamp: datetime.datetime
    detail: str = ""


@dataclasses.dataclass
class VectorSimulationResult:
    """What the kernel produces: fills, and the arithmetic it needed to decide them.

    **`cash` and `portfolio_value` are execution-time diagnostics, not the reported accounting.**
    The kernel has to track cash to answer "does this order fit", so the numbers exist; they are
    exposed because an invariant that is never checked is an invariant that is not held. But the
    portfolio a run reports is the one `ziplime.finance.domain.ledger.Ledger` builds from these
    fills, and where the two ever disagree the Ledger is right and this is a bug here.
    """

    index: pd.DatetimeIndex
    fills: list[VectorFill]
    rejects: list[VectorReject]
    instruments: list[str]
    cash: pd.Series
    portfolio_value: pd.Series
    positions: pd.DataFrame
    initial_cash: float
    timing: ExecutionTiming
    #: Entries on an instrument already held, under `accumulate=False`. Counted rather than
    #: recorded: with level signals these outnumber the fills many times over, and they are the
    #: expected case rather than a problem to investigate.
    no_ops: int = 0
    kernel_version: str = VECTOR_KERNEL_VERSION

    def fills_frame(self) -> pd.DataFrame:
        """The fills as a table, in the column vocabulary of the spec's §19."""
        if not self.fills:
            return pd.DataFrame(columns=[
                "instrument", "timestamp", "size", "price", "side", "fees", "amount",
                "sequence", "intent", "signal_timestamp"])
        return pd.DataFrame([{
            "instrument": fill.instrument,
            "timestamp": fill.timestamp,
            "size": abs(fill.amount),
            "price": fill.price,
            "side": fill.side,
            "fees": fill.commission,
            "amount": fill.amount,
            "sequence": fill.sequence,
            "intent": fill.intent,
            "signal_timestamp": fill.signal_timestamp,
        } for fill in self.fills])

    def rejects_frame(self) -> pd.DataFrame:
        if not self.rejects:
            return pd.DataFrame(columns=[
                "instrument", "timestamp", "intent", "reason", "wanted_amount",
                "signal_timestamp", "detail"])
        return pd.DataFrame([dataclasses.asdict(reject) for reject in self.rejects])

    def as_portfolio(self) -> "VectorPortfolio":
        """The compatibility view. See :class:`VectorPortfolio` before reaching for it."""
        return VectorPortfolio(self)

    def __len__(self) -> int:
        return len(self.index)


# ---------------------------------------------------------------------------------------------
# Compatibility. Temporary by intent -- see the spec's §17.
# ---------------------------------------------------------------------------------------------

class _Wrapper:
    def __init__(self, index: pd.DatetimeIndex):
        self.index = index


class _Orders:
    def __init__(self, records_readable: pd.DataFrame):
        self.records_readable = records_readable


class VectorPortfolio:
    """A :class:`VectorSimulationResult` wearing the shape `adapter.to_execution_result` grew
    around a vectorbt portfolio: ``.wrapper.index``, ``.orders.records_readable``, ``.value()``.

    It exists so the kernel could be dropped in without touching the adapter on the first pass,
    and it is scaffolding rather than architecture: the adapter now accepts a
    :class:`VectorSimulationResult` directly, and this remains only for code that was written
    against a vectorbt portfolio and has not moved yet. New code should not reach for it, and the
    column names below are vectorbt's vocabulary, which is exactly why this does not belong in
    the kernel's own contract.
    """

    def __init__(self, result: VectorSimulationResult):
        self._result = result
        self.wrapper = _Wrapper(result.index)
        frame = result.fills_frame()
        if frame.empty:
            records = pd.DataFrame(columns=["Column", "Timestamp", "Size", "Price", "Side", "Fees"])
        else:
            records = pd.DataFrame({
                "Column": frame["instrument"],
                "Timestamp": frame["timestamp"],
                "Size": frame["size"],
                "Price": frame["price"],
                "Side": frame["side"],
                "Fees": frame["fees"],
            })
        self.orders = _Orders(records)

    def value(self) -> pd.Series:
        return self._result.portfolio_value

    def cash(self) -> pd.Series:
        return self._result.cash

    @property
    def result(self) -> VectorSimulationResult:
        return self._result
