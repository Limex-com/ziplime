"""Cross section for cross."""

from ziplime.sweep.vectorized_containers import Vectorized


class CrossSection(Vectorized):
    """``d.values()`` of a SweepDict, or a comprehension over instruments: what ``sum``, ``max``,
    ``min``, ``any``, ``all`` and ``len`` reduce across the instruments of each combination."""

    def __init__(self, values, present) -> None:
        super().__init__([values], mask=present)
        self.values, self.present = values, present

    def sweep_len(self):
        return self.present.sum(axis=1, keepdims=True)
