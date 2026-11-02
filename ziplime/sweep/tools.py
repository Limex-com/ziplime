"""What the ``ziplime sweep`` commands do besides running a sweep file.

``init`` writes a sweep file for an algorithm file; ``check`` runs a small corner of the grid to
show how the strategy converts; ``doctor`` says which backends this machine has. The command line
in ``ziplime/__main__.py`` only parses options and prints.
"""
import datetime
import importlib.metadata
import importlib.util
import json
import os
import platform
from pathlib import Path
from typing import Any

from ziplime.sweep.params import ConfigPath, Range, SweepError

#: Combinations ``check`` runs at most: enough to show how the strategy converts and whether a
#: parameter changes anything, few enough to take seconds on the CPU
CHECK_COMBINATIONS = 16


# -- the command line's --grid and --set ----------------------------------------------------------

def parse_value(text: str) -> Any:
    """A command-line value: JSON where it parses (``5``, ``0.5``, ``true``, ``[1, 2]``), else the
    text itself."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def parse_assignment(text: str) -> tuple[str, str]:
    name, separator, value = text.partition("=")
    if not separator or not name.strip():
        raise SweepError(f"{text!r} is not NAME=VALUE.")
    return name.strip(), value.strip()


def parse_grid(text: str) -> tuple[str, Any]:
    """``--grid fast=5,10,20`` -- a list; ``--grid slow=30:90:10`` -- from 30 to 90 by 10
    (``30:90`` by 1)."""
    name, spec = parse_assignment(text)
    if ":" in spec and "," not in spec:
        parts = [parse_value(part) for part in spec.split(":")]
        if len(parts) not in (2, 3) or not all(isinstance(p, (int, float)) for p in parts):
            raise SweepError(f"--grid {text}: a range is FROM:TO or FROM:TO:STEP.")
        return name, {"from": parts[0], "to": parts[1], "step": parts[2] if len(parts) == 3 else 1}
    return name, [parse_value(part) for part in spec.split(",") if part.strip()]


# -- init -----------------------------------------------------------------------------------------

def config_class(algorithm_file: Path):
    """The algorithm file's AlgorithmConfig, loaded without running anything else of it."""
    from ziplime.config.base_algorithm_config import BaseAlgorithmConfig

    spec = importlib.util.spec_from_file_location("ziplime_sweep_init_strategy", algorithm_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    found = getattr(module, "AlgorithmConfig", None)
    if not (isinstance(found, type) and issubclass(found, BaseAlgorithmConfig)):
        raise SweepError(f"{algorithm_file.name} has no AlgorithmConfig: a sweep varies the "
                         f"fields of a class AlgorithmConfig(BaseAlgorithmConfig).")
    return found


def suggested_grid(config) -> dict[str, list]:
    """A starting grid around each number's default: half, the default and twice it."""
    grid = {}
    for name, field in config.model_fields.items():
        value = field.default
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if isinstance(value, int):
            values = sorted({max(1, value // 2), value, value * 2}) if value > 0 else [value]
        else:
            # A fraction (0 < x < 1: a weight, a share invested) stays a fraction
            top = min(1.0, value * 1.5) if 0 < value < 1 else value * 1.5
            values = sorted({round(value * 0.5, 6), value, round(top, 6)}) if value else [value]
        grid[name] = values
    return grid


def template(algorithm_file: Path, output: Path, *, bundle: str | None, start: datetime.date,
             end: datetime.date, calendar: str, backend: str) -> dict:
    """A sweep file for the algorithm file: its numeric parameters around their defaults."""
    grid = suggested_grid(config_class(algorithm_file))
    if not grid:
        raise SweepError(f"{algorithm_file.name}'s AlgorithmConfig has no numeric field to vary.")
    try:
        algorithm = os.path.relpath(algorithm_file.resolve(), output.resolve().parent)
    except ValueError:  # another drive on Windows
        algorithm = str(algorithm_file.resolve())
    if algorithm.startswith(".."):
        algorithm = str(algorithm_file.resolve())  # beside or below the sweep file reads better
    return {
        "algorithm_file": algorithm,
        **({"bundle": bundle} if bundle else {}),
        "start_date": str(start),
        "end_date": str(end),
        "trading_calendar": calendar,
        "total_cash": 100000,
        "equity_commission": {"type": "per_share", "cost": 0.005},
        "timing": "next_close",
        "grid": grid,
        "backend": backend,
        "sort_by": "sharpe",
        "min_fills": 5,
        "verify": True,
        "output_dir": "results",
    }


def default_backend() -> str:
    """The fastest backend this machine has."""
    if importlib.util.find_spec("mlx") is not None:
        return "mlx"
    if importlib.util.find_spec("cupy") is not None:
        return "cupy"
    return "cpu"


# -- check ----------------------------------------------------------------------------------------

def corner_of(grid: dict) -> dict:
    """The first and last value of each parameter: the grid's corners, at most
    CHECK_COMBINATIONS of them."""
    small, combinations = {}, 1
    for name, spec in grid.items():
        if isinstance(spec, ConfigPath):
            values = spec.values.values() if isinstance(spec.values, Range) else list(spec.values)
            ends = _ends(values, combinations)
            small[name] = spec.model_copy(update={"values": ends})
        else:
            values = spec.values() if isinstance(spec, Range) else \
                (list(spec) if isinstance(spec, list) else [spec])
            ends = _ends(values, combinations)
            small[name] = ends
        combinations *= len(ends)
    return small


def _ends(values: list, combinations: int) -> list:
    if len(values) < 2 or combinations * 2 > CHECK_COMBINATIONS:
        return values[:1]
    return [values[0], values[-1]]


# -- doctor ---------------------------------------------------------------------------------------

def doctor() -> list[tuple[str, str]]:
    """What this machine can sweep on: (what, state) lines."""
    import numpy

    rows = [("python", platform.python_version()),
            ("ziplime", f"{_version('ziplime') or '?'} (loaded from "
                        f"{Path(__import__('ziplime').__file__).parent})"),
            ("numpy / polars / pandas",
             f"{numpy.__version__} / {_version('polars')} / {_version('pandas')}"),
            ("CPU cores", str(os.cpu_count()))]
    pandas = _version("pandas")
    if pandas and not pandas.startswith("2."):
        rows.append(("warning", f"pandas {pandas}: ziplime needs pandas 2.x; pandas 3 changes "
                                f"results"))
    rows.append(("backend mlx", _mlx()))
    rows.append(("backend cupy", _cupy()))
    rows.append(("backend cpu / exact", f"numpy, float64, up to {os.cpu_count()} processes"))
    for name, module, distribution, use in (
            ("TA-Lib", "talib", "ta-lib", "talib.* on history rows"),
            ("scikit-learn", "sklearn", "scikit-learn", "models fitted in initialize"),
            ("numba", "numba", "numba", "the vector kernel of compute_signals entries")):
        installed = importlib.util.find_spec(module) is not None
        rows.append((name, f"{_version(distribution) or 'installed'} -- {use}" if installed
                     else f"not installed (needed for {use})"))
    rows.append(("suggested backend", default_backend()))
    return rows


def _version(distribution: str) -> str:
    try:
        return importlib.metadata.version(distribution)
    except Exception:
        return ""


def _mlx() -> str:
    if importlib.util.find_spec("mlx") is None:
        return 'not installed -- pip install "ziplime[mlx]" (Apple silicon), "mlx[cuda]" (NVIDIA)'
    try:
        import mlx.core as mx
        metal = bool(getattr(mx, "metal", None) and mx.metal.is_available())
        device = "Apple GPU (Metal)" if metal else str(mx.default_device())
        return f"{_version('mlx')} on {device}"
    except Exception as error:  # an install that cannot reach its GPU
        return f"installed, but fails: {type(error).__name__}: {str(error)[:100]}"


def _cupy() -> str:
    if importlib.util.find_spec("cupy") is None:
        return 'not installed -- pip install "ziplime[cuda]" (NVIDIA)'
    try:
        import cupy
        device = cupy.cuda.runtime.getDeviceProperties(0)
        name = device["name"].decode() if isinstance(device["name"], bytes) else device["name"]
        return f"{cupy.__version__} on {name}, {device['totalGlobalMem'] / 2 ** 30:.0f} GB"
    except Exception as error:
        return f"installed, but no GPU: {type(error).__name__}: {str(error)[:100]}"
