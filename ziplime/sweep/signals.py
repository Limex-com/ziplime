"""Signals for every combination, each distinct one computed once.

Two sources, as in the engine: indicators declared with ``context.indicators(...)`` in initialize,
or the strategy's ``compute_signals``. Either way most combinations share most of their signals --
``rolling_mean(5)`` is the same array whatever ``slow`` is -- and the sweep computes each distinct
signal once and hands every combination an index into the distinct ones.
"""
import numpy as np
import pandas as pd
import polars as pl

from ziplime.sweep.params import SweepError
from ziplime.vectorized.indicators import compute_indicators, long_frame, per_instrument
from ziplime.vectorized.signals import (
    LookAheadError, PricePanel, normalise_signals, verify_causality,
)

#: Signals the vector kernel trades on.
DECISION_NAMES = ("entries", "exits", "short_entries", "short_exits")


# -- compute_signals ---------------------------------------------------------------------------

def compute(algorithm, context, panel: PricePanel, config, decisions: bool = True) -> dict:
    """The algorithm's compute_signals with this config, normalised as the engine does it."""
    algorithm.config = config
    computed = normalise_signals(algorithm.compute_signals(context, panel), panel.index)
    missing = [name for name in ("entries", "exits") if name not in computed]
    if decisions and missing:
        raise SweepError(
            f"compute_signals did not return {missing}. Trading on signals needs \"entries\" (when "
            f"to open) and \"exits\" (when to close). It returned: {sorted(computed)}.")
    return computed


def check_causality(algorithm, context, panel: PricePanel, config, decisions: bool = True):
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


class _ReadTracker:
    """A config that records which of its fields compute_signals read."""

    def __init__(self, config):
        object.__setattr__(self, "_config", config)
        object.__setattr__(self, "read", set())

    def __getattr__(self, name):
        if name in type(self._config).model_fields:
            self.read.add(name)
        return getattr(self._config, name)


class SignalCache:
    """compute_signals once per distinct value of the config fields it reads.

    Rotation reads only lookback, so 360 combinations cost 12 calls.
    """

    def __init__(self, algorithm, context, panel, in_run, columns, decisions):
        self._args = (algorithm, context, panel, in_run, columns, decisions)
        self.fields = None
        self.variants = {}

    def key(self, config):
        return tuple((name, repr(getattr(config, name))) for name in sorted(self.fields))

    def get(self, config):
        if self.fields is not None and self.key(config) in self.variants:
            return self.key(config)
        algorithm, context, panel, in_run, columns, decisions = self._args
        tracker = _ReadTracker(config)
        arrays = signal_arrays(compute(algorithm, context, panel, tracker, decisions),
                               in_run, columns, decisions)
        algorithm.config = config
        if self.fields is None or not tracker.read <= self.fields:
            self.fields = (self.fields or set()) | tracker.read
            self.variants.clear()
        self.variants[self.key(config)] = arrays
        return self.key(config)

    def stacks(self, configs, n_bars: int, n_columns: int, decisions: bool = True):
        """decisions=True: each combination's decisions, (combinations, bars, instruments) per
        name, for the vector kernel. decisions=False: the distinct signal sets (variants, bars,
        instruments) and each combination's variant, so nothing is copied per combination."""
        for _ in range(3):  # until the set of fields read stops growing
            keys = [self.get(config) for config in configs]
            if all(key in self.variants for key in keys):
                break
        unique = list(dict.fromkeys(keys))
        names = list(dict.fromkeys(name for key in unique for name in self.variants[key]))

        def stack(key_list, name):
            return np.stack([self.variants[key].get(name, np.zeros((n_bars, n_columns), dtype=bool))
                             for key in key_list])

        if decisions:
            return {name: stack(keys, name) for name in names}, None
        position = {key: k for k, key in enumerate(unique)}
        return ({name: stack(unique, name) for name in names},
                np.array([position[key] for key in keys], dtype=np.int32))


# -- context.indicators ------------------------------------------------------------------------

class IndicatorSignals:
    """Every combination's indicators, each distinct expression computed once.

    Polars compares expressions without computing them: ``rolling_mean(5)`` from two combinations
    is one expression. Over moex_trend's 458 (fast, slow) pairs that is 39 means rather than 916.
    Every expression that reads no other indicator is computed in one polars query across all
    cores; one that does (``pl.col("momentum").rank().over("date")``) is distinct also by which
    ``momentum`` it read.
    """

    def __init__(self, initialized: list, panel: PricePanel, in_run: np.ndarray, columns: list[str]):
        self.names = list(initialized[0].indicators)
        distinct = {id(each): each for each in initialized}
        if any(list(each.indicators) != self.names for each in distinct.values()):
            raise SweepError("The set of indicators differs between combinations.")
        unique = {}      # (serialized expression, what it reads) -> id
        sources = []     # id -> (expression, {indicator it reads: that indicator's id})
        keys_of = {}     # id(Initialized) -> {name: expression id}
        for each in distinct.values():
            keys = {}
            for name, expression in each.indicators.items():
                if not isinstance(expression, pl.Expr):
                    raise SweepError(f"Indicator {name!r} has to be a polars expression, "
                                     f"not {type(expression).__name__}.")
                reads = {other: keys[other] for other in expression.meta.root_names() if other in keys}
                key = (expression.meta.serialize(format="json"), tuple(sorted(reads.items())))
                if key not in unique:
                    unique[key] = len(sources)
                    sources.append((expression, reads))
                keys[name] = unique[key]
            keys_of[id(each)] = keys
        self.keys = [keys_of[id(each)] for each in initialized]

        frame = long_frame(panel)
        frame = frame.with_columns([per_instrument(expression).alias(f"_k{k}")
                                    for k, (expression, reads) in enumerate(sources) if not reads])
        for k, (expression, reads) in enumerate(sources):
            if reads:
                inputs = frame.with_columns(pl.col(f"_k{read}").alias(name)
                                            for name, read in reads.items())
                frame = frame.with_columns(inputs.select(per_instrument(expression).alias(f"_k{k}")))
        order = pd.Index(panel.columns).get_indexer(columns)
        n_panel = len(panel.index)
        self.arrays = [frame[f"_k{k}"].cast(pl.Float64).to_numpy()
                       .reshape(len(panel.columns), n_panel).T[np.ix_(in_run, order)]
                       for k in range(len(sources))]

    @property
    def variants(self) -> int:
        return len(self.arrays)

    def stacks(self, indices):
        """Each indicator's distinct arrays among these combinations, and each one's index."""
        stacks, index = {}, {}
        for name in self.names:
            keys = [self.keys[i][name] for i in indices]
            unique = list(dict.fromkeys(keys))
            position = {key: k for k, key in enumerate(unique)}
            stacks[name] = np.stack([self.arrays[key] for key in unique])
            index[name] = np.array([position[key] for key in keys], dtype=np.int32)
        return stacks, index


def check_indicators(expressions: dict, panel: PricePanel, warmup: int, config):
    """verify_causality for one combination's indicators."""
    computed = normalise_signals(compute_indicators(expressions, panel), panel.index)
    try:
        verify_causality(lambda prices: compute_indicators(expressions, prices),
                         panel=panel, computed=computed, warmup=warmup)
    except LookAheadError as error:
        raise SweepError(f"Look-ahead with {config.model_dump()}:\n{error}") from None
