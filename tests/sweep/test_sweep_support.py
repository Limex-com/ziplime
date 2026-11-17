"""Shared sweep helpers preserve identity across the API and converter boundary."""

import subprocess
import sys
import types
from pathlib import Path

import numpy as np
import pytest

from ziplime.sweep import api, convert, cross, engine, frames, lazy, runner
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
from ziplime.sweep import api, convert
from ziplime.sweep.all_assets import ALL
assert api.ALL is convert.ALL is ALL
assert convert.Context is api.Context
assert convert.Data is api.Data
assert api.Costs is importlib.import_module('ziplime.sweep.costs').Costs
assert api.Windows is importlib.import_module('ziplime.sweep.windows').Windows
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


def test_legacy_imports_share_the_extracted_objects() -> None:
    for module in (api, convert, cross, frames, lazy):
        assert module.ALL is ALL
    assert convert._AllAssets is type(ALL)
    for module in (api, convert, cross, engine, frames, lazy, runner):
        assert module.NotConvertible is NotConvertible
    for module in (api, convert, cross, frames):
        assert module.Vectorized is Vectorized
    for module in (api, convert, cross):
        assert module.VectorUniverse is VectorUniverse
    for module in (api, convert, cross, engine, lazy):
        assert module.is_array is is_array


def test_legacy_engine_types_share_the_extracted_objects() -> None:
    from ziplime.sweep.costs import Costs
    from ziplime.sweep.errors import Rediscover
    from ziplime.sweep.windows import Windows

    assert api.Costs is engine.Costs is Costs
    assert api.Windows is engine.Windows is lazy.Windows is runner.Windows is Windows
    assert engine.Rediscover is lazy.Rediscover is Rediscover


def test_api_and_cross_selection_use_the_same_all_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(type(ALL), "_columns", None)
    columns = api.Columns({"A": 10, "B": 20}, Backend("numpy"))
    values = np.array([[1.0, 2.0], [3.0, 4.0]])

    np.testing.assert_array_equal(convert.ALL == "B", [[False, True]])
    np.testing.assert_array_equal(cross.ALL != 10, [[False, True]])
    np.testing.assert_array_equal(cross.column_mask("B"), [[False, True]])
    assert columns.select(values, convert.ALL) is values
    assert columns.only(cross.ALL) is True
    assert cross.column_mask(api.ALL) is True
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
    with pytest.raises(convert.NotConvertible, match="has no single .exchange"):
        ALL.exchange


def test_array_detection_supports_backend_arrays_and_excludes_numpy_scalars() -> None:
    assert is_array(np.array([[1, 2]]))
    assert is_array(np.array(1))
    assert is_array(types.SimpleNamespace(shape=(2, 3)))
    assert not is_array(np.float64(1.0))
    assert not is_array(np.int64(1))
    assert not is_array(1.0)
    assert not is_array([1, 2])
