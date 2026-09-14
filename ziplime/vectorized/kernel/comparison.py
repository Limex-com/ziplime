"""Two execution results, side by side, on the things that would make them different runs.

The spec's §32 asks for this as a test utility, and that is what it is for now. It is written as
library code rather than as a test helper because the question it answers -- "did the fast engine
and the careful one agree?" -- is the question the hybrid in §46 is built around, and a research
loop that screens vectorised and validates event-driven will want to ask it in production.

What it deliberately does not do is decide whether a difference is acceptable. A tolerance that
lives inside a comparison function becomes the definition of "the same", and different callers
mean different things by that: a parity test wants exact agreement, a research screen wants to
know whether the ranking survived. So this reports the differences and the caller judges them.
"""
from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd


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


def _transactions(result) -> list[tuple]:
    """Every fill in a `TradingAlgorithmExecutionResult`, as comparable tuples.

    Rounded, because the two engines reach the same price by different arithmetic and an
    unrounded float comparison would report a difference of 1e-16 as a missing trade.
    """
    out = []
    for row in result.perf["transactions"]:
        for transaction in row:
            symbol = getattr(getattr(transaction, "asset", None), "symbol", None)
            out.append((transaction.dt, symbol, round(float(transaction.amount), 8),
                        round(float(transaction.price), 8)))
    return sorted(out, key=lambda item: (str(item[0]), str(item[1])))


def _equity(result) -> pd.Series:
    return pd.Series(result.perf["portfolio_value"].to_numpy(dtype=float),
                     index=result.perf.index)


def compare_execution_results(vector_result, event_result) -> ExecutionComparison:
    """Compare a vectorised run against an event-driven one.

    Both arguments are `TradingAlgorithmExecutionResult`s -- the vector one having been through
    `to_execution_result`, so that the two are measured by the same metric set rather than by each
    engine's own conventions. Comparing a kernel result directly against an event result would be
    comparing a fills table against a performance table.

    Args:
        vector_result: The run produced through the vector kernel.
        event_result: The run produced by the event-driven engine.

    Returns:
        An :class:`ExecutionComparison`. It reports; it does not assert.
    """
    vector_fills, event_fills = _transactions(vector_result), _transactions(event_result)
    vector_set, event_set = set(vector_fills), set(event_fills)

    vector_equity, event_equity = _equity(vector_result), _equity(event_result)
    shared = vector_equity.index.intersection(event_equity.index)
    if len(shared):
        gap = (vector_equity.loc[shared] - event_equity.loc[shared]).abs()
        max_gap = float(gap.max())
        gap_at = gap.idxmax()
    else:
        max_gap, gap_at = float("nan"), None

    def last(series, column):
        frame = series.perf
        return float(frame[column].iloc[-1]) if column in frame.columns and len(frame) else float("nan")

    return ExecutionComparison(
        vector_trades=len(vector_fills),
        event_trades=len(event_fills),
        only_in_vector=sorted(vector_set - event_set, key=str),
        only_in_event=sorted(event_set - vector_set, key=str),
        ending_equity_vector=float(vector_equity.iloc[-1]) if len(vector_equity) else float("nan"),
        ending_equity_event=float(event_equity.iloc[-1]) if len(event_equity) else float("nan"),
        ending_return_vector=last(vector_result, "algorithm_period_return"),
        ending_return_event=last(event_result, "algorithm_period_return"),
        max_equity_gap=max_gap,
        max_equity_gap_at=gap_at,
        bars_compared=len(shared),
    )
