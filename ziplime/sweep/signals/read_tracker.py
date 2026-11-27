"""Read tracker for signals."""

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig


class _ReadTracker:
    """A config that records which of its fields compute_signals read."""

    def __init__(self, config: BaseAlgorithmConfig) -> None:
        object.__setattr__(self, "_config", config)
        object.__setattr__(self, "read", set())

    def __getattr__(self, name: str):
        if name in type(self._config).model_fields:
            self.read.add(name)
        return getattr(self._config, name)
