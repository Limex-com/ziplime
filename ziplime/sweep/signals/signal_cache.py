"""Signal cache for signals."""

from collections.abc import Sequence
import numpy as np
from ziplime.vectorized.signals import PricePanel
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.sweep.initialize import SweepContext
from ziplime.sweep.signals.read_tracker import _ReadTracker
from ziplime.sweep.signals.compute_utils import compute
from ziplime.sweep.signals.compute_utils import signal_arrays


class SignalCache:
    """compute_signals once per distinct value of the config fields it reads.

    Rotation reads only lookback, so 360 combinations cost 12 calls.
    """

    def __init__(
        self,
        algorithm: AlgorithmFile,
        context: SweepContext,
        panel: PricePanel,
        in_run: np.ndarray,
        columns: list[str],
        decisions: bool,
    ) -> None:
        self._args = (algorithm, context, panel, in_run, columns, decisions)
        self.fields = None
        self.variants = {}

    def key(self, config: BaseAlgorithmConfig):
        return tuple((name, repr(getattr(config, name))) for name in sorted(self.fields))

    def get(self, config: BaseAlgorithmConfig):
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

    def stacks(self, configs: Sequence[BaseAlgorithmConfig], n_bars: int, n_columns: int, decisions: bool = True):
        """decisions=True: each combination's decisions, (combinations, bars, instruments) per
        name, for the vector kernel. decisions=False: the distinct signal sets (variants, bars,
        instruments) and each combination's variant, so nothing is copied per combination."""
        for _ in range(3):  # until the set of fields read stops growing
            keys = [self.get(config) for config in configs]
            if all(key in self.variants for key in keys):
                break
        unique = list(dict.fromkeys(keys))
        names = list(dict.fromkeys(name for key in unique for name in self.variants[key]))

        def stack(key_list, name: str):
            return np.stack([self.variants[key].get(name, np.zeros((n_bars, n_columns), dtype=bool))
                             for key in key_list])

        if decisions:
            return {name: stack(keys, name) for name in names}, None
        position = {key: k for k, key in enumerate(unique)}
        return ({name: stack(unique, name) for name in names},
                np.array([position[key] for key in keys], dtype=np.int32))
