import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("module", [
    "ziplime.pipeline.data.bound_column_descr",
    "ziplime.pipeline.domain_inference",
    "ziplime.pipeline.mixins",
    "ziplime.pipeline.universal_mixin",
    "ziplime.pipeline.terms.boolean_term",
    "ziplime.pipeline.terms.computable_term",
    "ziplime.pipeline.terms.asset_exists",
    "ziplime.pipeline.terms.filters.filter",
    "ziplime.pipeline.terms.filters",
    "ziplime.pipeline.terms.filters.maximum_filter",
    "ziplime.pipeline.terms.filters.percentile_filter",
    "ziplime.pipeline.terms.factors.factor",
    "ziplime.pipeline.terms.classifiers.classifier",
    "ziplime.pipeline.graph",
    "ziplime.pipeline.visualize",
    "ziplime.utils.api_support",
    "ziplime.utils.events",
    "ziplime.utils.event_rule",
    "ziplime.trading.base_trading_algorithm",
])
def test_pipeline_and_api_entry_points_import_in_a_fresh_process(module: str):
    result = subprocess.run(
        [sys.executable, "-c", f"import importlib; importlib.import_module({module!r})"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_pipeline_graph_import_defers_visualization() -> None:
    script = """
import sys
from typing import get_type_hints
from ziplime.pipeline.graph import TermGraph
assert 'ziplime.pipeline.visualize' not in sys.modules
from ziplime.pipeline import visualize
assert visualize.TermGraph is TermGraph
assert get_type_hints(visualize.display_graph)['g'] is TermGraph
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
