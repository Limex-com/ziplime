"""Concrete factor filters retain their public exports and construction behavior."""

import subprocess
import sys
from pathlib import Path
from typing import get_type_hints

import numpy as np
import pandas as pd
import pytest

from ziplime.errors import BadPercentileBounds
from ziplime.pipeline import Classifier, Factor, Filter
from ziplime.pipeline.terms import filters
from ziplime.pipeline.terms.filters.maximum_filter import MaximumFilter
from ziplime.pipeline.terms.filters.percentile_filter import PercentileFilter


@pytest.mark.parametrize("name, module", [
    ("MaximumFilter", "maximum_filter"),
    ("PercentileFilter", "percentile_filter"),
])
def test_concrete_filter_exports_load_only_when_requested(name: str, module: str) -> None:
    script = f"""
import importlib
import sys
from ziplime.pipeline.terms import filters
for name in ('MaximumFilter', 'PercentileFilter'):
    assert name in dir(filters)
    assert name in filters.__all__
    assert name not in vars(filters)
for module in ('maximum_filter', 'percentile_filter'):
    assert 'ziplime.pipeline.terms.filters.' + module not in sys.modules
exported = getattr(filters, {name!r})
module = importlib.import_module('ziplime.pipeline.terms.filters.' + {module!r})
assert exported is getattr(module, {name!r})
assert vars(filters)[{name!r}] is exported
other = 'percentile_filter' if {module!r} == 'maximum_filter' else 'maximum_filter'
assert 'ziplime.pipeline.terms.filters.' + other not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True,
        timeout=20, check=False,
    )

    assert result.returncode == 0, result.stderr


def test_filter_star_import_preserves_all_public_exports() -> None:
    namespace = {}
    exec("from ziplime.pipeline.terms.filters import *", namespace)

    for name in filters.__all__:
        assert namespace[name] is getattr(filters, name)
    assert namespace["MaximumFilter"] is MaximumFilter
    assert namespace["PercentileFilter"] is PercentileFilter


def test_unknown_filter_export_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="has no attribute 'unknown_filter'"):
        getattr(filters, "unknown_filter")


def test_concrete_filter_imports_and_annotations_use_the_existing_domain_types() -> None:
    assert filters.MaximumFilter is MaximumFilter
    assert filters.PercentileFilter is PercentileFilter
    assert get_type_hints(MaximumFilter.__new__)["factor"] is Factor
    assert get_type_hints(MaximumFilter.__new__)["groupby"] == Classifier | None
    assert get_type_hints(PercentileFilter.__new__)["factor"] is Factor


def test_factor_top_constructs_and_computes_the_ungrouped_maximum_filter() -> None:
    factor = Factor(inputs=(), window_length=0, dtype=float)
    mask_term = Filter(inputs=(), window_length=0)
    maximum = factor.top(1, mask=mask_term)
    dates = pd.date_range("2024-01-02", periods=2, tz="UTC")
    assets = pd.Index([1, 2, 3, 4, 5])
    values = np.array([[1.0, 3.0, 2.0, 4.0, np.nan], [5.0, 1.0, np.nan, 2.0, 9.0]])
    mask = np.array([[True, True, True, False, True], [True, False, True, True, False]])
    group_labels = maximum.inputs[1]._compute([], dates, assets, mask)

    assert type(maximum) is MaximumFilter
    assert maximum.inputs[0] is factor
    assert maximum.mask is mask_term
    assert maximum is factor.top(1, mask=mask_term)
    np.testing.assert_array_equal(
        maximum._compute([values, group_labels], dates, assets, mask),
        [[False, True, False, False, False], [True, False, False, False, False]],
    )


def test_factor_top_preserves_classifier_grouping_and_masks() -> None:
    factor = Factor(inputs=(), window_length=0, dtype=float)
    groupby = Classifier(inputs=(), window_length=0, dtype=np.int64, missing_value=-1)
    maximum = factor.top(1, groupby=groupby)
    values = np.array([[1.0, 3.0, 2.0, 4.0, np.nan], [5.0, 1.0, np.nan, 2.0, 9.0]])
    labels = np.array([[0, 0, 1, 1, -1], [0, 0, 1, 1, 1]], dtype=np.int64)
    mask = np.array([[True, True, True, False, True], [True, False, True, True, False]])

    assert maximum.inputs[1] is groupby
    assert maximum is factor.top(1, groupby=groupby)
    np.testing.assert_array_equal(
        maximum._compute(
            [values, labels], pd.date_range("2024-01-02", periods=2, tz="UTC"), pd.Index([1, 2, 3, 4, 5]), mask,
        ),
        [[False, True, True, False, False], [True, False, False, True, False]],
    )


def test_factor_percentile_filter_preserves_bounds_masks_and_memoization() -> None:
    factor = Factor(inputs=(), window_length=0, dtype=float)
    mask_term = Filter(inputs=(), window_length=0)
    percentile = factor.percentile_between(25, 75, mask=mask_term)
    values = np.array([[1.0, 2.0, 3.0, 4.0, 5.0], [10.0, 20.0, 30.0, 40.0, np.nan]])
    mask = np.array([[True, True, False, True, True], [True, False, True, True, True]])

    assert type(percentile) is PercentileFilter
    assert percentile.inputs[0] is factor
    assert percentile.mask is mask_term
    assert percentile is factor.percentile_between(25, 75, mask=mask_term)
    np.testing.assert_array_equal(
        percentile._compute(
            [values], pd.date_range("2024-01-02", periods=2, tz="UTC"), pd.Index([1, 2, 3, 4, 5]), mask,
        ),
        [[False, True, False, True, False], [False, False, True, False, False]],
    )


@pytest.mark.parametrize("lower, upper", [(-1, 50), (75, 25), (0, 101)])
def test_percentile_bounds_validation_is_preserved(lower: float, upper: float) -> None:
    factor = Factor(inputs=(), window_length=0, dtype=float)

    with pytest.raises(BadPercentileBounds):
        factor.percentile_between(lower, upper)
