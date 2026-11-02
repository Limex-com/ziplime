"""Many signal sets through the kernel in one call: a parameter sweep, on the CPU or the GPU.

    from ziplime.vectorized.kernel import ExecutionTiming, simulate_signals_batch

    batch = simulate_signals_batch(
        prices=closes, entries=entries, exits=exits,     # entries, exits: (combinations, T, N)
        labels=grid, size=100, execution=ExecutionTiming.next_close(),
        backend="mlx",
    )
    best = batch.final_value.idxmax()
    exact = batch.rerun(best)    # simulate_signals on that one set: float64, every fill

A sweep asks one question many times: the same prices and costs, a different set of signals each
time. :func:`~ziplime.vectorized.kernel.simulate_signals` answers it one set at a time, and the
``exact`` backend is exactly that -- a loop, kept so the other backend has something to be held to.

The ``mlx`` backend answers every set at once on the Apple GPU, one thread per set. The kernel
does not vectorise *within* a run: whether an order fits depends on the cash every fill before it
left, so bar 200 cannot be decided before bar 199. But separate runs share nothing, and a GPU runs
thousands of sequential loops side by side. Each thread takes the decisions
:func:`ziplime.vectorized.kernel._core.run_events` takes, in the same order.

**What the GPU gives up is float64.** Metal has no double precision, so cash and prices are
float32 there -- about seven significant digits, a cent or so on a six-figure account. That is
ample to rank combinations and not enough to report one, so the two jobs are kept apart: a batch
result carries summaries -- value curves, counts, fees -- and never fills, and
:meth:`BatchSimulationResult.rerun` runs the chosen set through the float64 kernel for the fills
:func:`~ziplime.vectorized.to_execution_result` replays. The rerun is the one that counts.

On positions in whole units the two paths take the same decisions, fill for fill. They can part in
two ways, both at the edge of float32's resolution:

* an order whose cost lands within rounding of the cash left fits on one path and not the other;
* a fractional position -- a volume cap of 10% on 23 units buys 2.3 -- can be closed down to a
  residue of about 1e-15 units in float64 and to exactly zero in float32. The exact kernel closes
  that residue as one more fill; the GPU sees a flat book. Under a minimum commission that dust
  fill also pays the minimum, and the values part by that much.

Signals arrive stacked, ``(combinations, bars, instruments)``, rather than as a list of frames: at
a thousand combinations a list of DataFrames is most of the memory and most of the time. All the
rest -- prices, sizes, volumes, costs, timing -- is shared by every set and validated exactly as
:func:`~ziplime.vectorized.kernel.simulate_signals` validates it.
"""
from __future__ import annotations

import dataclasses
import typing
from typing import Callable, Literal

import numpy as np
import pandas as pd
import structlog

from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.slippage.slippage_model import SlippageModel

from ziplime.vectorized.kernel import costs
from ziplime.vectorized.kernel.models import (
    VECTOR_KERNEL_VERSION, ExecutionTiming, RejectReason, VectorKernelError,
    VectorSimulationResult,
)
from ziplime.vectorized.kernel.simulation import (
    _REJECT_NAMES, _as_frame, _bool_frame, _execution_prices, _normalise_prices,
    simulate_signals,
)

_logger = structlog.get_logger(__name__)

Backend = Literal["exact", "mlx"]

#: The columns of :attr:`BatchSimulationResult.rejects`, one per reason a run can record.
REJECT_REASONS: tuple[str, ...] = typing.get_args(RejectReason)

_SIGNAL_NAMES = ("entries", "exits", "short_entries", "short_exits")


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

    def rerun(self, label) -> VectorSimulationResult:
        """One combination through :func:`simulate_signals`: float64, every fill and rejection.

        This is the result to report, replay through the ledger or compare with an event-driven
        run. The batch's own numbers for the same label are what it was ranked by.
        """
        return self._rerun(label)

    def __len__(self) -> int:
        return len(self.labels)


def simulate_signals_batch(
    prices,
    entries=None,
    exits=None,
    *,
    short_entries=None,
    short_exits=None,
    labels=None,
    size=1.0,
    initial_cash: float = 100_000.0,
    execution: ExecutionTiming | None = None,
    opens=None,
    volumes=None,
    commission: CommissionModel | None = None,
    slippage: SlippageModel | None = None,
    assets: dict | None = None,
    allow_short: bool = False,
    accumulate: bool = False,
    backend: Backend = "exact",
) -> BatchSimulationResult:
    """Run every signal set through the kernel and summarise each one.

    Args:
        prices: Close prices, ``T x N`` indexed by time, shared by every combination.
        entries: ``(combinations, T, N)`` booleans -- a NumPy or MLX array, or a list of
            ``T x N`` frames aligned with ``prices``. One slice per combination, read exactly as
            :func:`simulate_signals` reads its ``entries``. The same goes for ``exits``,
            ``short_entries`` and ``short_exits``; any left out is all False.
        labels: One per combination and unique, typically the parameters that produced it. They
            index every result. Defaults to ``0 .. combinations - 1``.
        backend: ``"exact"`` runs :func:`simulate_signals` once per combination, in float64.
            ``"mlx"`` runs all of them at once on the GPU, in float32. It needs the ``mlx``
            package, which exists for Apple silicon only, and is refused rather than quietly
            replaced when that is missing: the two do not produce identical numbers.

        The remaining arguments are :func:`simulate_signals`'s, applied to every combination.

    Returns:
        A :class:`BatchSimulationResult`.

    Raises:
        VectorKernelError: for anything :func:`simulate_signals` refuses, for signal stacks that
            disagree about their shape, and for a backend this machine cannot run.
    """
    if backend not in typing.get_args(Backend):
        raise VectorKernelError(
            f"backend={backend!r} is not a backend. Use 'exact' (float64, one combination at a "
            f"time) or 'mlx' (float32, all at once on the GPU).")
    if backend == "mlx":
        from ziplime.vectorized.kernel import _mlx

        # Before any validation: on a machine without a GPU path, nothing else about the call
        # is worth reporting first.
        _mlx.require()
    prices = _normalise_prices(prices, "prices")
    index, columns = prices.index, list(prices.columns)
    if len(index) == 0:
        raise VectorKernelError("prices has no bars, so there is nothing to simulate.")
    timing = execution or ExecutionTiming.next_open()

    # The same order as `simulate_signals`: shapes, then permissions, then costs, then the one
    # prerequisite check, so a mistake is reported against the argument that holds it.
    stacks = _signal_stacks(
        {"entries": entries, "exits": exits, "short_entries": short_entries,
         "short_exits": short_exits}, index, columns)
    if not allow_short and (stacks["short_entries"].any() or stacks["short_exits"].any()):
        raise VectorKernelError(
            "short_entries/short_exits were given but allow_short is False. Shorting changes what "
            "a strategy risks, so it is opted into rather than inferred from a signal array.")
    labels = _labels(labels, stacks["entries"].shape[0])

    sizes = _as_frame(size, index, columns, "size", dtype=float)
    volume_frame = (_as_frame(volumes, index, columns, "volumes", dtype=float)
                    if volumes is not None else None)
    slip = costs.resolve_slippage(slippage)
    commission_model = costs.resolve_commission(commission)
    execution_prices = _execution_prices(timing, prices, opens, index, columns)

    def rerun(label) -> VectorSimulationResult:
        position = _position_of(labels, label)
        frames = {name: pd.DataFrame(stacks[name][position], index=index, columns=columns)
                  for name in _SIGNAL_NAMES}
        return simulate_signals(
            prices=prices, **frames, size=size, initial_cash=initial_cash, execution=timing,
            opens=opens, volumes=volumes, commission=commission, slippage=slippage,
            assets=assets, allow_short=allow_short, accumulate=accumulate)

    if backend == "exact":
        summary = _run_exact(rerun, labels, len(index), len(columns))
        precision = "float64"
    else:
        summary = _mlx.run_batch(
            stacks, execution_prices.to_numpy(dtype=float), prices[columns].to_numpy(dtype=float),
            sizes.to_numpy(dtype=float),
            volume_frame.to_numpy(dtype=float) if volume_frame is not None else None,
            slip, commission_model, float(initial_cash), accumulate, timing.bar_offset)
        summary["rejects"] = _rejects_by_reason(summary.pop("codes"), stacks, timing)
        precision = "float32"

    combos = pd.Index(labels)
    _logger.info("Vector kernel simulated a batch", combinations=len(labels), bars=len(index),
                 instruments=len(columns), backend=backend, timing=timing.execute_at,
                 version=VECTOR_KERNEL_VERSION)
    return BatchSimulationResult(
        index=index, instruments=columns, labels=labels,
        portfolio_value=pd.DataFrame(summary["value"], index=index, columns=combos),
        cash=pd.Series(summary["cash"], index=combos, name="cash"),
        positions=pd.DataFrame(summary["positions"], index=combos, columns=columns),
        fills=pd.Series(summary["fills"], index=combos, name="fills"),
        fees=pd.Series(summary["fees"], index=combos, name="fees"),
        no_ops=pd.Series(summary["no_ops"], index=combos, name="no_ops"),
        rejects=pd.DataFrame(summary["rejects"], index=combos, columns=list(REJECT_REASONS)),
        initial_cash=float(initial_cash), timing=timing, backend=backend, precision=precision,
        _rerun=rerun)


def _signal_stacks(given: dict, index: pd.DatetimeIndex, columns: list[str]) -> dict:
    """Every signal argument as a boolean ``(combinations, T, N)`` array, all the same shape."""
    stacks = {name: _signal_stack(value, name, index, columns)
              for name, value in given.items() if value is not None}
    if not stacks:
        raise VectorKernelError(
            "No signals were given, so there is nothing to sweep. Pass entries and exits as "
            "(combinations, bars, instruments) arrays.")
    counts = {name: stack.shape[0] for name, stack in stacks.items()}
    if len(set(counts.values())) > 1:
        raise VectorKernelError(
            f"The signal stacks disagree about how many combinations there are: {counts}. Slice "
            f"k of every stack is combination k, so they have to be the same length.")
    n_combos = next(iter(counts.values()))
    if n_combos == 0:
        raise VectorKernelError("The signal stacks hold no combinations.")
    absent = np.zeros((n_combos, len(index), len(columns)), dtype=np.bool_)
    return {name: stacks.get(name, absent) for name in given}


def _signal_stack(value, name: str, index: pd.DatetimeIndex, columns: list[str]) -> np.ndarray:
    if isinstance(value, pd.DataFrame):
        value = [value]
    if isinstance(value, (list, tuple)):
        if not all(isinstance(item, pd.DataFrame) for item in value):
            raise VectorKernelError(
                f"{name} is a list, so it has to be a list of DataFrames, one per combination.")
        if not value:
            return np.zeros((0, len(index), len(columns)), dtype=np.bool_)
        return np.stack([_bool_frame(item, index, columns, f"{name}[{k}]").to_numpy()
                         for k, item in enumerate(value)])

    array = np.asarray(value)
    expected = (len(index), len(columns))
    if array.ndim != 3 or array.shape[1:] != expected:
        hint = (" A 2-D array is one combination's signals; add the leading axis "
                "(array[None]) or call simulate_signals." if array.ndim == 2 else "")
        raise VectorKernelError(
            f"{name} has shape {array.shape}; expected (combinations, {expected[0]}, "
            f"{expected[1]}) -- one slice per combination, bars and instruments as in prices."
            f"{hint}")
    if array.dtype == np.bool_:
        return array
    if array.dtype.kind in "iu":
        return array != 0
    if array.dtype.kind == "f":
        # NaN is "no signal", as `fillna(False)` makes it on the single-run path. `!= 0` alone
        # would read every NaN warm-up row as a signal.
        return (array != 0) & ~np.isnan(array)
    raise VectorKernelError(f"{name} has dtype {array.dtype}; signals have to be booleans.")


def _labels(labels, n_combos: int) -> list:
    if labels is None:
        return list(range(n_combos))
    labels = list(labels)
    if len(labels) != n_combos:
        raise VectorKernelError(
            f"{len(labels)} labels for {n_combos} combinations; there has to be one each.")
    try:
        unique = len(set(labels)) == len(labels)
    except TypeError:
        raise VectorKernelError(
            "labels have to be hashable -- tuples rather than lists -- because they index the "
            "results.") from None
    if not unique:
        raise VectorKernelError(
            "labels repeat, so rerun(label) could not say which combination it means.")
    return labels


def _position_of(labels: list, label) -> int:
    try:
        return labels.index(label)
    except ValueError:
        raise KeyError(f"No combination is labelled {label!r}.") from None


def _run_exact(rerun, labels: list, n_bars: int, n_columns: int) -> dict[str, np.ndarray]:
    """The reference: :func:`simulate_signals` once per combination, summarised."""
    n_combos = len(labels)
    summary = {
        "value": np.empty((n_bars, n_combos)),
        "cash": np.empty(n_combos),
        "positions": np.empty((n_combos, n_columns)),
        "fees": np.empty(n_combos),
        "fills": np.empty(n_combos, dtype=np.int64),
        "no_ops": np.empty(n_combos, dtype=np.int64),
        "rejects": np.zeros((n_combos, len(REJECT_REASONS)), dtype=np.int64),
    }
    reason_column = {reason: k for k, reason in enumerate(REJECT_REASONS)}
    for position, label in enumerate(labels):
        result = rerun(label)
        summary["value"][:, position] = result.portfolio_value.to_numpy()
        summary["cash"][position] = float(result.cash.iloc[-1])
        summary["positions"][position] = result.positions.iloc[-1].to_numpy()
        summary["fees"][position] = sum(fill.commission for fill in result.fills)
        summary["fills"][position] = len(result.fills)
        summary["no_ops"][position] = result.no_ops
        for reject in result.rejects:
            summary["rejects"][position, reason_column[reject.reason]] += 1
    return summary


def _rejects_by_reason(codes: np.ndarray, stacks: dict, timing: ExecutionTiming) -> np.ndarray:
    """The GPU's counts by `_core` code, regrouped under the names the exact path reports.

    `no_execution_bar` is not decided in the loop at all: `simulate_signals` rejects every signal
    on the last bar before the loop starts when the fill would land on the bar after it, whatever
    the position, and the same count is taken here from the stacks.
    """
    rejects = np.zeros((codes.shape[0], len(REJECT_REASONS)), dtype=np.int64)
    for code, reason in _REJECT_NAMES.items():
        rejects[:, REJECT_REASONS.index(reason)] += codes[:, code]
    if timing.bar_offset:
        late = sum(stacks[name][:, -timing.bar_offset:, :].sum(axis=(1, 2))
                   for name in _SIGNAL_NAMES)
        rejects[:, REJECT_REASONS.index("no_execution_bar")] = late
    return rejects
