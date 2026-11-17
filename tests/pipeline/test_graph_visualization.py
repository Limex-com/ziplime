from unittest.mock import Mock

import pytest

from ziplime.pipeline import visualize
from ziplime.pipeline.graph import TermGraph


@pytest.mark.parametrize("format_", ["jpeg", "png", "svg"])
def test_graph_image_properties_return_the_requested_rendering(
    format_: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = TermGraph({})
    rendered = object()
    display_graph = Mock(return_value=rendered)
    monkeypatch.setattr(visualize, "display_graph", display_graph)

    assert getattr(graph, format_) is rendered
    display_graph.assert_called_once_with(graph, format_)
