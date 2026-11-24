"""Testing progress publisher for progress."""

from collections import namedtuple
from ziplime.pipeline.hooks.progress.progress_model import ProgressModel


class TestingProgressPublisher:
    """A progress publisher that records a trace of model states for testing."""

    TraceState = namedtuple(
        "TraceState",
        [
            "state",
            "percent_complete",
            "execution_bounds",
            "current_chunk_bounds",
            "current_work",
        ],
    )

    def __init__(self) -> None:
        self.trace = []

    def publish(self, model: ProgressModel) -> None:
        self.trace.append(
            self.TraceState(
                state=model.state,
                percent_complete=model.percent_complete,
                execution_bounds=model.execution_bounds,
                current_chunk_bounds=model.current_chunk_bounds,
                current_work=model.current_work,
            ),
        )
