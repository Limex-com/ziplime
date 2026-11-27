"""Indicator signals for signals."""

from collections.abc import Sequence
import numpy as np
import pandas as pd
import polars as pl
from ziplime.sweep.params import SweepError
from ziplime.vectorized.indicators import long_frame, per_instrument
from ziplime.vectorized.signals import PricePanel


class IndicatorSignals:
    """Every combination's indicators, each distinct expression computed once.

    Polars compares expressions without computing them: ``rolling_mean(5)`` from two combinations
    is one expression. Over moex_trend's 458 (fast, slow) pairs that is 39 means rather than 916.
    Every expression that reads no other indicator is computed in one polars query across all
    cores; one that does (``pl.col("momentum").rank().over("date")``) is distinct also by which
    ``momentum`` it read.
    """

    def __init__(self, initialized: list, panel: PricePanel, in_run: np.ndarray, columns: list[str]) -> None:
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

    def stacks(self, indices: Sequence[int]):
        """Each indicator's distinct arrays among these combinations, and each one's index."""
        stacks, index = {}, {}
        for name in self.names:
            keys = [self.keys[i][name] for i in indices]
            unique = list(dict.fromkeys(keys))
            position = {key: k for k, key in enumerate(unique)}
            stacks[name] = np.stack([self.arrays[key] for key in unique])
            index[name] = np.array([position[key] for key in keys], dtype=np.int32)
        return stacks, index
