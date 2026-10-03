"""Running a strategy vectorised, and reporting it as a ziplime result.

A vectorised engine computes over whole arrays at once; ziplime's own engine walks bar by bar.
The two answer different questions well -- a parameter sweep over a thousand combinations is
hopeless bar by bar, and a strategy that reads its own portfolio, rolls a futures contract or
settles an option at expiry cannot be expressed as arrays at all.

There are two ways to use one from the other, and they are for different problems.

**Vectorise the whole run, then report it as ziplime.**
:func:`~ziplime.vectorized.kernel.simulate_signals` turns signals into fills, and
:func:`to_execution_result` replays those fills through the ordinary ledger and metric set,
returning exactly the
:class:`~ziplime.trading.trading_algorithm_execution_result.TradingAlgorithmExecutionResult` an
event-driven run produces. This is the one for a parameter sweep: try everything vectorised, then
report the winner in the same terms as every other backtest. What it gives up is the part of a
strategy that reads its own book -- a sizing rule, an exposure cap, a roll -- because a boolean
signal array cannot carry one.

**Vectorise half of one run.** :mod:`ziplime.vectorized.signals` lets a strategy compute its
indicators over the whole history in a single pass, before the first bar, and then trade them bar
by bar through the ordinary blotter. Execution, slippage, commissions and portfolio-dependent
logic are all untouched; only the arithmetic moves. This is the one for a strategy whose signals
are array work but whose decisions are not.

Neither needs a third-party engine. The kernel is ziplime's own, and `numba` only compiles its
inner loop -- absent, the same loop runs interpreted and reports the same numbers.

``vectorbt`` is not a dependency of either. :func:`to_execution_result` still accepts a
``vectorbt`` portfolio, and that is deliberate: replaying one through this ledger is how a second
engine's fills are made comparable with ziplime's own, which is what
``tests/reference/vectorbt/`` uses it for. It is a testing and benchmarking capability, not a
part of the vectorised path.
"""
from ziplime.vectorized.signals import (
    LookAheadError, PricePanel, SignalPanel, verify_causality,
)

__all__ = [
    "ExecutionTiming",
    "LookAheadError",
    "PricePanel",
    "ReconciliationError",
    "SignalPanel",
    "VectorKernelError",
    "VectorSimulationResult",
    "VectorizedRun",
    "replay_transactions",
    "simulate_signals",
    "to_execution_result",
    "verify_causality",
]

#: Imported on demand. `adapter` reaches back into `ziplime.trading`, which imports this package
#: for the signal half, and eager imports here would close that loop at interpreter start.
_LAZY = {"ReconciliationError", "VectorizedRun", "replay_transactions", "to_execution_result"}

#: The native kernel. Lazy for the same reason and one more: it pulls in the cost models, which a
#: strategy using only the signal half has no need of.
_LAZY_KERNEL = {"ExecutionTiming", "VectorKernelError", "VectorSimulationResult",
                "simulate_signals"}


def __getattr__(name: str):
    if name in _LAZY:
        from ziplime.vectorized import adapter

        return getattr(adapter, name)
    if name in _LAZY_KERNEL:
        from ziplime.vectorized import kernel

        return getattr(kernel, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(__all__)
