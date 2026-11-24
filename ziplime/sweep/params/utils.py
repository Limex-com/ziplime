"""Utils for params."""

import copy
import itertools
from ziplime.sweep.params.config_path import ConfigPath
from ziplime.sweep.params.range import Range
from ziplime.sweep.params.sweep_error import SweepError


def config_paths(grid: dict) -> dict[str, str]:
    """``{grid name: path in the config}``."""
    return {name: spec.path if isinstance(spec, ConfigPath) else name
            for name, spec in grid.items()}


def config_for(shared: dict, combo: dict, paths: dict[str, str]) -> dict:
    """One combination's config: the shared part with the combination's values written in."""
    data = copy.deepcopy(shared)
    for name, value in combo.items():
        *parents, last = paths[name].split(".")
        node = data
        try:
            for part in parents:
                node = node[int(part)] if isinstance(node, list) else node[part]
            if isinstance(node, list):
                node[int(last)] = value
            else:
                node[last] = value
        except (KeyError, IndexError, ValueError, TypeError):
            raise SweepError(
                f'Path "{paths[name]}" (parameter "{name}") is not in the config. Nested values '
                f"come from config_file -- check that it has them.") from None
    return data


def expand_grid(grid: dict, where: str | None) -> list[dict]:
    """Every combination of the grid's values that passes ``where``."""
    axes = {}
    for name, spec in grid.items():
        if isinstance(spec, ConfigPath):
            spec = spec.values
        values = spec.values() if isinstance(spec, Range) else spec
        axes[name] = values if isinstance(values, list) else [values]
        if not axes[name]:
            raise SweepError(f'Grid parameter "{name}" has no values.')
    combos = (dict(zip(axes, values)) for values in itertools.product(*axes.values()))
    if where:
        try:
            test = compile(where, "<where>", "eval")
            combos = [c for c in combos if eval(test, {"__builtins__": {}}, c)]
        except NameError as error:
            raise SweepError(f"Unknown name in where: {error}. The grid has: {list(grid)}") from None
    else:
        combos = list(combos)
    if not combos:
        raise SweepError("No combination passes the where condition.")
    return combos
