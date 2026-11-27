"""Meboot for synthetic."""

import numpy as np
from ziplime.sweep.synthetic.base_generator import Generator
from ziplime.sweep.synthetic.sampling_utils import meboot


class Meboot(Generator):
    """Vinod's maximum entropy bootstrap of each instrument's log close over the run, anchored at
    the last real close before it."""

    def __init__(self, *args) -> None:
        super().__init__(*args)
        close = self.feats.close
        self.levels = []
        for column in range(close.shape[1]):
            series = np.log(close[self.start - 1:, column])
            first = int(np.argmax(np.isfinite(series))) if np.isfinite(series).any() else None
            self.levels.append((first, series[first:] if first is not None else None))

    def path(self, rng: np.random.Generator):
        returns = np.zeros((self.length, len(self.levels)))
        for column, (first, levels) in enumerate(self.levels):
            if levels is None or len(levels) < 3:
                continue
            replicate = meboot(levels, rng, trim=self.family.trim,
                               expand_sd=self.family.expand_sd)[0]
            steps = np.diff(replicate - replicate[0] + levels[0])
            if self.family.drift == "zero":
                steps = steps - np.diff(levels).mean()
            returns[first:, column] = steps
        return np.arange(self.start, self.start + self.length), returns
