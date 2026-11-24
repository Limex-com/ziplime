"""The comparison between vector and event-driven results."""

from __future__ import annotations
import dataclasses


@dataclasses.dataclass
class ExecutionComparison:
    """What differs between two runs of what should be the same strategy."""

    vector_trades: int
    event_trades: int
    #: Fills present in one run and not the other, as (timestamp, instrument, amount) tuples.
    only_in_vector: list[tuple]
    only_in_event: list[tuple]
    ending_equity_vector: float
    ending_equity_event: float
    ending_return_vector: float
    ending_return_event: float
    #: Largest gap between the two equity curves at any bar they share.
    max_equity_gap: float
    #: Where that gap is, which is usually the trade that caused it.
    max_equity_gap_at: object = None
    bars_compared: int = 0

    @property
    def trade_count_difference(self) -> int:
        return self.vector_trades - self.event_trades

    @property
    def ending_equity_difference(self) -> float:
        return self.ending_equity_vector - self.ending_equity_event

    @property
    def ending_return_difference(self) -> float:
        return self.ending_return_vector - self.ending_return_event

    @property
    def agrees(self) -> bool:
        """Exact agreement, to floating-point noise. The strict reading, on purpose.

        A caller wanting a looser one reads the numbers below; a caller wanting "close enough"
        has to say how close, which is not a decision this can take for them.
        """
        return (self.trade_count_difference == 0
                and not self.only_in_vector and not self.only_in_event
                and abs(self.ending_equity_difference) < 1e-6
                and self.max_equity_gap < 1e-6)

    def report(self) -> str:
        """A few lines a person can read, in the order the differences matter."""
        lines = [
            f"trades          vector {self.vector_trades}  event {self.event_trades}  "
            f"(difference {self.trade_count_difference:+d})",
            f"ending equity   vector {self.ending_equity_vector:,.6f}  "
            f"event {self.ending_equity_event:,.6f}  "
            f"(difference {self.ending_equity_difference:+,.6f})",
            f"ending return   vector {self.ending_return_vector:+.6%}  "
            f"event {self.ending_return_event:+.6%}  "
            f"(difference {self.ending_return_difference:+.6%})",
            f"max equity gap  {self.max_equity_gap:,.6f}"
            + (f" at {self.max_equity_gap_at}" if self.max_equity_gap_at is not None else ""),
            f"bars compared   {self.bars_compared}",
        ]
        if self.only_in_vector:
            lines.append(f"only in vector  {self.only_in_vector[:5]}"
                         + (" ..." if len(self.only_in_vector) > 5 else ""))
        if self.only_in_event:
            lines.append(f"only in event   {self.only_in_event[:5]}"
                         + (" ..." if len(self.only_in_event) > 5 else ""))
        return "\n".join(lines)
