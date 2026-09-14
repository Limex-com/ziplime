"""ziplime's own vector simulation kernel: signals in, fills out.

    from ziplime.vectorized.kernel import simulate_signals, ExecutionTiming

    result = simulate_signals(
        prices=closes, entries=entries, exits=exits, size=100,
        initial_cash=100_000.0, execution=ExecutionTiming.next_open(), opens=opens,
    )

What it replaces is one thing: the step that turns signals into orders and fills, which used to
be `vectorbt.Portfolio.from_signals`. Everything after that was already ziplime's --
:class:`~ziplime.finance.domain.ledger.Ledger` for cash and positions,
:class:`~ziplime.finance.metrics_tracker.MetricsTracker` for the result -- and none of it moves.

What it deliberately is not: a portfolio accounting engine. The kernel tracks cash only far enough
to decide whether an order fits, and the ledger remains the single source of truth for what a run
holds. See :mod:`ziplime.vectorized.kernel.simulation` for the four semantic decisions it takes on
its own, and why each is not vectorbt's.
"""
from .comparison import ExecutionComparison, compare_execution_results
from .costs import ResolvedSlippage, resolve_commission, resolve_slippage
from .models import (
    VECTOR_KERNEL_VERSION, ExecutionTiming, VectorFill, VectorKernelError, VectorPortfolio,
    VectorReject, VectorSimulationResult,
)
from .simulation import simulate_signals

__all__ = [
    "VECTOR_KERNEL_VERSION",
    "ExecutionComparison",
    "ExecutionTiming",
    "ResolvedSlippage",
    "VectorFill",
    "VectorKernelError",
    "VectorPortfolio",
    "VectorReject",
    "VectorSimulationResult",
    "compare_execution_results",
    "resolve_commission",
    "resolve_slippage",
    "simulate_signals",
]
