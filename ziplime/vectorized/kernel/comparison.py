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


import pandas as pd
from ziplime.trading.trading_algorithm_execution_result import TradingAlgorithmExecutionResult
from ziplime.vectorized.kernel.execution_comparison import ExecutionComparison


def _transactions(result: TradingAlgorithmExecutionResult) -> list[tuple]:
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


def _equity(result: TradingAlgorithmExecutionResult) -> pd.Series:
    return pd.Series(result.perf["portfolio_value"].to_numpy(dtype=float),
                     index=result.perf.index)


def compare_execution_results(
    vector_result: TradingAlgorithmExecutionResult,
    event_result: TradingAlgorithmExecutionResult,
) -> ExecutionComparison:
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

    def last(series: TradingAlgorithmExecutionResult, column: str):
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
