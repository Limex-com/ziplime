"""Synthetic for params."""

from typing import Literal
from pydantic import model_validator
from ziplime.sweep.params.family import Family
from ziplime.sweep.params.strict import Strict


class Synthetic(Strict):
    """The sweep run again on synthetic price paths made from the real bars::

        "synthetic": {"paths": 200, "families": [{"method": "stationary", "block": 20},
                                                 {"method": "meboot"}, {"method": "garch"}]}

    Every family's paths start from the real bars at ``start_date`` (the warm-up before it stays
    real); every combination runs on every path."""

    paths: int = 100
    seed: int = 0
    families: list[Family] = [Family(method="stationary")]
    #: The bars a path is resampled or fitted from: ``history`` -- everything loaded, warm-up
    #: included; ``run`` -- from ``start_date`` on
    source: Literal["history", "run"] = "history"
    #: How a combination's paths are summed up into one number to choose by: ``median``,
    #: ``mean``, ``min``, or a percentile ``p5``, ``p10``, ``p25``...
    select_by: str = "median"

    @model_validator(mode="after")
    def check(self):
        if self.paths < 1:
            raise ValueError("paths has to be at least 1")
        if not self.families:
            raise ValueError("families is empty")
        labels = [family.label for family in self.families]
        if len(set(labels)) != len(labels):
            raise ValueError(f"two families share a name: {labels}; give them names")
        statistic = self.select_by
        if statistic not in ("median", "mean", "min") and not (
                statistic.startswith("p") and statistic[1:].replace(".", "", 1).isdigit()
                and 0 <= float(statistic[1:]) <= 100):
            raise ValueError("select_by is median, mean, min or a percentile such as p10")
        return self
