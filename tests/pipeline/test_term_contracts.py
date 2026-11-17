"""Boolean masks and shared mixin specialization preserve pipeline term behavior."""

from typing import get_type_hints

import numpy as np
import pandas as pd
import pytest

from ziplime.errors import UnsupportedDataType
from ziplime.pipeline import Classifier, CustomClassifier, CustomFactor, CustomFilter, Factor, Filter
from ziplime.pipeline.graph import TermGraph
from ziplime.pipeline.mixins import AliasedMixin, CustomTermMixin, DownsampledMixin, IfElseMixin
from ziplime.pipeline.mixins import UniversalMixin as LegacyUniversalMixin
from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.pipeline.terms.boolean_term import BooleanTerm
from ziplime.pipeline.terms.computable_term import ComputableTerm
from ziplime.pipeline.universal_mixin import UniversalMixin


class ConstantFactor(CustomFactor):
    inputs = ()
    window_length = 1

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray) -> None:
        out[:] = 2.0


class ConstantFilter(CustomFilter):
    inputs = ()
    window_length = 1

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray) -> None:
        out[:] = True


class ConstantClassifier(CustomClassifier):
    inputs = ()
    window_length = 1
    dtype = np.dtype("int64")
    missing_value = -1

    def compute(self, today: pd.Timestamp, assets: pd.Index, out: np.ndarray) -> None:
        out[:] = 7


@pytest.fixture(params=[
    Factor(inputs=(), window_length=0, dtype=float),
    Filter(inputs=(), window_length=0),
    Classifier(inputs=(), window_length=0, dtype=np.int64, missing_value=-1),
], ids=["factor", "filter", "classifier"])
def term(request: pytest.FixtureRequest) -> ComputableTerm:
    return request.param


def test_masks_and_mixin_annotations_resolve_without_concrete_filter_imports() -> None:
    assert get_type_hints(ComputableTerm.__new__)["mask"] == BooleanTerm | None
    assert get_type_hints(ComputableTerm._init)["mask"] is BooleanTerm
    assert get_type_hints(ComputableTerm._static_identity)["mask"] is BooleanTerm
    assert get_type_hints(CustomTermMixin.__new__)["mask"] == BooleanTerm | None
    assert get_type_hints(IfElseMixin.__new__)["condition"] is BooleanTerm
    assert get_type_hints(ComputableTerm._with_mixin)["mixin_type"] == type[UniversalMixin]


def test_default_mask_is_the_shared_boolean_root(term: ComputableTerm) -> None:
    assert term.mask is AssetExists()
    assert isinstance(term.mask, BooleanTerm)
    assert term.mask.dtype == np.dtype(bool)
    assert term.dependencies[term.mask] == 0


@pytest.mark.parametrize("mask", [AssetExists(), Filter(inputs=(), window_length=0)], ids=["root", "filter"])
def test_explicit_masks_preserve_identity_and_graph_dependencies(term: ComputableTerm, mask: BooleanTerm) -> None:
    kwargs = dict(inputs=(), window_length=0, dtype=term.dtype, missing_value=term.missing_value, mask=mask)
    masked = type(term)(**kwargs)

    assert masked.mask is mask
    assert masked is type(term)(**kwargs)
    assert masked.dependencies[mask] == 0
    ordered = list(TermGraph({"value": masked}).ordered())
    positions = {id(node): index for index, node in enumerate(ordered)}
    assert positions[id(mask)] < positions[id(masked)]


def test_aliases_preserve_specialization_and_shared_cache(term: ComputableTerm) -> None:
    alias = term.alias("selected")

    assert LegacyUniversalMixin is UniversalMixin
    assert isinstance(alias, type(term))
    assert alias.inputs == (term,)
    assert alias.name == "selected"
    assert alias is term.alias("selected")
    assert type(alias) is AliasedMixin.universal_mixin_specialization(type(term))
    assert UniversalMixin._UNIVERSAL_MIXIN_SUBTYPES[(AliasedMixin, type(term))] is type(alias)


def test_downsampling_preserves_the_wrapped_term_and_specialization(term: ComputableTerm) -> None:
    downsampled = term.downsample("week_start")

    assert isinstance(downsampled, type(term))
    assert downsampled._wrapped_term is term
    assert downsampled._frequency == "week_start"
    assert downsampled.dtype == term.dtype
    assert downsampled is term.downsample("week_start")
    assert type(downsampled) is DownsampledMixin.universal_mixin_specialization(type(term))


def test_conditional_terms_preserve_principal_type_and_computation(term: ComputableTerm) -> None:
    condition = Filter(inputs=(), window_length=0)
    when_true, when_false = term.alias("true"), term.alias("false")
    selected = condition.if_else(when_true, when_false)
    choices = np.array([[True, False]])
    true_values = np.array([[1, 0]], dtype=term.dtype)
    false_values = np.array([[0, 1]], dtype=term.dtype)

    assert isinstance(selected, type(term))
    assert selected.inputs == (condition, when_true, when_false)
    result = selected._compute(
        [choices, true_values, false_values],
        dates=pd.date_range("2024-01-02", periods=1, tz="UTC"),
        assets=pd.Index([1, 2]), mask=np.ones((1, 2), dtype=bool),
    )
    np.testing.assert_array_equal(result, np.ones((1, 2), dtype=term.dtype))


@pytest.mark.parametrize("term, value", [
    (Factor(inputs=(), window_length=0, dtype=float), 5.0),
    (Classifier(inputs=(), window_length=0, dtype=np.int64, missing_value=-1), 5),
])
def test_fillna_preserves_constant_and_conditional_specializations(term: ComputableTerm, value: int | float) -> None:
    filled = term.fillna(value)

    assert isinstance(filled, type(term))
    assert filled.inputs[1] is term
    assert filled.inputs[2].params["const"] == value
    assert filled is term.fillna(value)


@pytest.mark.parametrize("term_type, value", [
    (ConstantFactor, 2.0), (ConstantFilter, True), (ConstantClassifier, 7),
])
@pytest.mark.parametrize("mask", [AssetExists(), Filter(inputs=(), window_length=0)], ids=["root", "filter"])
def test_custom_terms_compute_only_for_selected_assets(
    term_type: type[CustomFactor | CustomFilter | CustomClassifier], value: int | float,
    mask: BooleanTerm,
) -> None:
    custom = term_type(mask=mask)
    selected = np.array([[True, False], [False, True]])
    result = custom._compute(
        [], pd.date_range("2024-01-02", periods=2, tz="UTC"), pd.Index([1, 2]), selected,
    )

    assert custom.mask is mask
    expected = np.full(selected.shape, custom.missing_value, dtype=custom.dtype)
    expected[selected] = value
    np.testing.assert_array_equal(result, expected)


def test_filters_still_reject_non_boolean_dtypes() -> None:
    with pytest.raises(UnsupportedDataType):
        Filter(inputs=(), window_length=0, dtype=float)
