"""The result of a batch of vector simulations."""

from __future__ import annotations
import dataclasses
from typing import Callable
from collections.abc import Hashable
import pandas as pd
from ziplime.vectorized.kernel.models import VECTOR_KERNEL_VERSION, ExecutionTiming, VectorSimulationResult


@dataclasses.dataclass
class BatchSimulationResult:
    """One summary per signal set: enough to rank them, not enough to report one.

    Every frame and series here has one entry per combination, under the labels the caller
    passed. Fills are absent on purpose -- see the module docstring -- and :meth:`rerun` is how to
    get them for the combination worth looking at.
    """

    index: pd.DatetimeIndex
    instruments: list[str]
    labels: list
    #: Account value at every bar, ``T x combinations``. The kernel's working number, as on
    #: :class:`~ziplime.vectorized.kernel.VectorSimulationResult`, not the ledger's.
    portfolio_value: pd.DataFrame
    #: Cash after the last bar.
    cash: pd.Series
    #: Units held after the last bar, ``combinations x instruments``.
    positions: pd.DataFrame
    fills: pd.Series
    fees: pd.Series
    no_ops: pd.Series
    #: Rejections counted by reason, ``combinations x`` :data:`REJECT_REASONS`.
    rejects: pd.DataFrame
    initial_cash: float
    timing: ExecutionTiming
    backend: str
    #: ``float64`` on the exact backend, ``float32`` on the GPU.
    precision: str
    kernel_version: str = VECTOR_KERNEL_VERSION
    _rerun: Callable[[object], VectorSimulationResult] | None = dataclasses.field(
        default=None, repr=False)

    @property
    def final_value(self) -> pd.Series:
        return self.portfolio_value.iloc[-1]

    @property
    def total_return(self) -> pd.Series:
        return self.final_value / self.initial_cash - 1.0

    def rerun(self, label: Hashable) -> VectorSimulationResult:
        """One combination through :func:`simulate_signals`: float64, every fill and rejection.

        This is the result to report, replay through the ledger or compare with an event-driven
        run. The batch's own numbers for the same label are what it was ranked by.
        """
        return self._rerun(label)

    def __len__(self) -> int:
        return len(self.labels)
