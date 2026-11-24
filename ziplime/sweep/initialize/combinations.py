"""Combinations for initialize."""

from collections.abc import Iterable
from pydantic import ValidationError
from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.sweep.params import SweepError, config_for


class Combinations:
    """The grid's combinations and their configs, built when needed.

    A million validated configs held at once cost gigabytes; a combination is kept as the few grid
    values that make it, and its config is built for the batch that needs it.
    """

    def __init__(
        self,
        combos: list[dict],
        shared: dict,
        paths: dict[str, str],
        config_class: type[BaseAlgorithmConfig],
    ) -> None:
        self.combos, self.shared, self.paths, self.config_class = combos, shared, paths, config_class

    def __len__(self) -> int:
        return len(self.combos)

    def config(self, index: int):
        try:
            return self.config_class.model_validate(
                config_for(self.shared, self.combos[index], self.paths))
        except ValidationError as error:
            raise SweepError(f"Grid values {self.combos[index]} do not fit "
                             f"{self.config_class.__name__}:\n{error}") from None

    def configs(self, indices: Iterable[int]) -> list:
        return [self.config(index) for index in indices]

    def key(self, index: int, fields: set[str]) -> tuple:
        """The combination's values of the grid parameters under ``fields`` of the config."""
        combo = self.combos[index]
        return tuple(combo[name] for name, path in self.paths.items()
                     if path.split(".")[0] in fields)
