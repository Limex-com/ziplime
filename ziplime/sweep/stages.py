"""Timing stages for sweep reporting."""

import time
from collections import defaultdict


class Stages:
    """Where the time went: data, signals, the first GPU batch (compilation included), the rest."""

    def __init__(self) -> None:
        self.began = time.perf_counter()
        self.seconds = defaultdict(float)

    def add(self, name: str, seconds: float) -> None:
        self.seconds[name] += seconds

    def report(self) -> str:
        total = time.perf_counter() - self.began
        rows = dict(self.seconds)
        rows["other (metrics, sorting, files)"] = max(0.0, total - sum(rows.values()))
        lines = [f"  {name:<44} {seconds:8.2f} s" for name, seconds in rows.items()]
        return "Stages:\n" + "\n".join(lines) + f"\n  {'total':<44} {total:8.2f} s"
