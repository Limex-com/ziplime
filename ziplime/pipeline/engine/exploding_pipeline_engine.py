"""Exploding pipeline engine for engine."""

import datetime
from ziplime.pipeline.pipeline import Pipeline
from ziplime.pipeline.hooks import PipelineHooks
from ziplime.pipeline.engine.no_engine_registered import NoEngineRegistered
from ziplime.pipeline.engine.pipeline_engine import PipelineEngine


class ExplodingPipelineEngine(PipelineEngine):
    """A PipelineEngine that doesn't do anything."""

    def run_pipeline(self, pipeline: Pipeline, start_date: datetime.date, end_date: datetime.date,
                     hooks: list[PipelineHooks] = None):
        raise NoEngineRegistered(
            "Attempted to run a pipeline but no pipeline " "resources were registered."
        )

    def run_chunked_pipeline(
            self, pipeline: Pipeline, start_date: datetime.date, end_date: datetime.date, chunksize: int,
            hooks: list[PipelineHooks] = None
    ):
        raise NoEngineRegistered(
            "Attempted to run a chunked pipeline but no pipeline "
            "resources were registered."
        )
