"""Compute utils for signals."""

import numpy as np
import pandas as pd
from ziplime.sweep.params import SweepError
from ziplime.vectorized.indicators import compute_indicators
from ziplime.vectorized.signals import LookAheadError, PricePanel, normalise_signals, verify_causality
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.sweep.initialize import SweepContext
from ziplime.sweep.signals.support import DECISION_NAMES
from ziplime.sweep.signals.read_tracker import _ReadTracker


def compute(
    algorithm: AlgorithmFile,
    context: SweepContext,
    panel: PricePanel,
    config: 'BaseAlgorithmConfig | _ReadTracker',
    decisions: bool = True,
) -> dict:
    """The algorithm's compute_signals with this config, normalised as the engine does it."""
    algorithm.config = config
    computed = normalise_signals(algorithm.compute_signals(context, panel), panel.index)
    missing = [name for name in ("entries", "exits") if name not in computed]
    if decisions and missing:
        raise SweepError(
            f"compute_signals did not return {missing}. Trading on signals needs \"entries\" (when "
            f"to open) and \"exits\" (when to close). It returned: {sorted(computed)}.")
    return computed


def check_causality(
    algorithm: AlgorithmFile,
    context: SweepContext,
    panel: PricePanel,
    config: 'BaseAlgorithmConfig | _ReadTracker',
    decisions: bool = True,
) -> None:
    """ziplime's verify_causality: no signal may read the future."""
    computed = compute(algorithm, context, panel, config, decisions)
    try:
        verify_causality(lambda prices: algorithm.compute_signals(context, prices),
                         panel=panel, computed=computed, warmup=algorithm.warmup)
    except LookAheadError as error:
        raise SweepError(f"Look-ahead with {config.model_dump()}:\n{error}") from None


def signal_arrays(computed: dict, in_run: np.ndarray, columns: list[str],
                  decisions: bool = True) -> dict:
    """Signals over the run's bars, columns in the order of context.universe.

    decisions=True -- only entries/exits/..., as booleans (NaN is no signal); decisions=False --
    every signal as it is, warm-up NaN included, for handle_data to read.

    numpy rather than pandas: this runs once per distinct signal set, and on thousands of them
    pandas operations on small frames cost more than the indicators themselves.
    """
    arrays = {}
    for name in (DECISION_NAMES if decisions else computed):
        if name not in computed:
            continue
        frame = computed[name]
        if not isinstance(frame, pd.DataFrame):
            raise SweepError(f'"{name}" has to be a frame (bars × instruments), '
                             f"not {type(frame).__name__}.")
        order = frame.columns.get_indexer(columns)
        if (order < 0).any():
            raise SweepError(f'"{name}" lacks columns {[c for c, i in zip(columns, order) if i < 0]}.')
        values = frame.to_numpy()[np.ix_(in_run, order)]
        if values.dtype != np.bool_:
            values = (np.where(pd.isna(values), False, values).astype(bool) if decisions
                      else values.astype(float))
        arrays[name] = values
    return arrays


def check_indicators(expressions: dict, panel: PricePanel, warmup: int, config: BaseAlgorithmConfig) -> None:
    """verify_causality for one combination's indicators."""
    computed = normalise_signals(compute_indicators(expressions, panel), panel.index)
    try:
        verify_causality(lambda prices: compute_indicators(expressions, prices),
                         panel=panel, computed=computed, warmup=warmup)
    except LookAheadError as error:
        raise SweepError(f"Look-ahead with {config.model_dump()}:\n{error}") from None
