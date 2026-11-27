"""Shared sweep helpers preserve identity across the API and converter boundary."""

import subprocess
import sys
import types
from pathlib import Path

import numpy as np
import pytest

from ziplime.sweep import api, cross
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.array_utils import is_array
from ziplime.sweep.backends import Backend
from ziplime.sweep.errors import NotConvertible
from ziplime.sweep.vectorized_containers import VectorUniverse, Vectorized


@pytest.mark.parametrize("entry_point", [
    "ziplime.sweep.api",
    "ziplime.sweep.convert",
    "ziplime.sweep.cross",
    "ziplime.sweep.costs",
    "ziplime.sweep.engine",
    "ziplime.sweep.lazy",
    "ziplime.sweep.windows",
])
def test_sweep_entry_points_import_in_any_order(entry_point: str) -> None:
    script = f"""
import importlib
import sys
importlib.import_module({entry_point!r})
if {entry_point!r} == 'ziplime.sweep.api':
    assert 'ziplime.sweep.convert' not in sys.modules
    assert 'ziplime.sweep.engine' not in sys.modules
from ziplime.sweep.api import Context, Data
from ziplime.sweep.api.context import Context as ContextType
from ziplime.sweep.api.data import Data as DataType
from ziplime.sweep.convert import Runtime
from ziplime.sweep.convert.runtime import Runtime as RuntimeType
assert Context is ContextType
assert Data is DataType
assert Runtime is RuntimeType
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_support_modules_import_without_api_or_converter() -> None:
    script = """
import sys
from ziplime.sweep import all_assets, array_utils, costs, errors, vectorized_containers, windows
assert 'ziplime.sweep.api' not in sys.modules
assert 'ziplime.sweep.convert' not in sys.modules
assert 'ziplime.sweep.engine' not in sys.modules
assert vectorized_containers.ALL is all_assets.ALL
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("package, implementation, name", [
    ("ziplime.sweep.api", "ziplime.sweep.api.context", "Context"),
    ("ziplime.sweep.api", "ziplime.sweep.api.data", "Data"),
    ("ziplime.sweep.convert", "ziplime.sweep.convert.runtime", "Runtime"),
    ("ziplime.sweep.convert", "ziplime.sweep.convert.converted", "Converted"),
    ("ziplime.sweep.cross", "ziplime.sweep.cross.cross_section", "CrossSection"),
    ("ziplime.sweep.cross", "ziplime.sweep.cross.ranking_value", "Ranking"),
    ("ziplime.sweep.engine", "ziplime.sweep.engine.hooks", "Hooks"),
    ("ziplime.sweep.engine", "ziplime.sweep.engine.result", "Result"),
    ("ziplime.sweep.frames", "ziplime.sweep.frames.table", "Table"),
    ("ziplime.sweep.lazy", "ziplime.sweep.lazy.lazy", "Lazy"),
    ("ziplime.sweep.lazy", "ziplime.sweep.lazy.lazy_registry", "LazyRegistry"),
])
def test_package_exports_use_their_own_implementation_types(
    package: str, implementation: str, name: str,
) -> None:
    import importlib

    assert getattr(importlib.import_module(package), name) is getattr(
        importlib.import_module(implementation), name,
    )


def test_api_and_cross_selection_use_the_same_all_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(type(ALL), "_columns", None)
    columns = api.Columns({"A": 10, "B": 20}, Backend("numpy"))
    values = np.array([[1.0, 2.0], [3.0, 4.0]])

    np.testing.assert_array_equal(ALL == "B", [[False, True]])
    np.testing.assert_array_equal(ALL != 10, [[False, True]])
    np.testing.assert_array_equal(cross.column_mask("B"), [[False, True]])
    assert columns.select(values, ALL) is values
    assert columns.only(ALL) is True
    assert cross.column_mask(ALL) is True
    assert ALL.sid is ALL


def test_vectorized_universe_emits_the_canonical_all() -> None:
    universe = VectorUniverse({"A": 10, "B": 20})

    assert len(universe) == 2
    assert universe["B"] == 20
    assert list(universe)[0] is ALL
    assert universe.keys()[0] is ALL
    assert universe.values()[0] is ALL
    name, asset = universe.items()[0]
    assert name is asset is ALL
    assert isinstance(universe.items(), Vectorized)
    mask = np.array([[True, False]])
    vectorized = Vectorized([ALL], mask=mask)
    assert vectorized.mask is mask


def test_extracted_sentinel_errors_are_caught_by_legacy_exception_import() -> None:
    with pytest.raises(NotConvertible, match="has no single .exchange"):
        ALL.exchange


def test_array_detection_supports_backend_arrays_and_excludes_numpy_scalars() -> None:
    assert is_array(np.array([[1, 2]]))
    assert is_array(np.array(1))
    assert is_array(types.SimpleNamespace(shape=(2, 3)))
    assert not is_array(np.float64(1.0))
    assert not is_array(np.int64(1))
    assert not is_array(1.0)
    assert not is_array([1, 2])
