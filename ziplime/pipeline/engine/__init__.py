"""
Computation engines for executing Pipelines.

This module defines the core computation algorithms for executing Pipelines.

The primary entrypoint of this file is SimplePipelineEngine.run_pipeline, which
implements the following algorithm for executing pipelines:

1. Determine the domain of the pipeline. The domain determines the
   top-level set of dates and assets that serve as row- and
   column-labels for the computations performed by this
   pipeline. This logic lives in
   ziplime.pipeline.domain.infer_domain.

2. Build a dependency graph of all terms in `pipeline`, with
   information about how many extra rows each term needs from its
   inputs. At this point we also **specialize** any generic
   LoadableTerms to the domain determined in (1). This logic lives in
   ziplime.pipeline.graph.TermGraph and
   ziplime.pipeline.graph.ExecutionPlan.

3. Combine the domain computed in (2) with our AssetFinder to produce
   a "lifetimes matrix". The lifetimes matrix is a DataFrame of
   booleans whose labels are dates x assets. Each entry corresponds
   to a (date, asset) pair and indicates whether the asset in
   question was tradable on the date in question. This logic
   primarily lives in AssetFinder.lifetimes.

4. Call self._populate_initial_workspace, which produces a
   "workspace" dictionary containing cached or otherwise pre-computed
   terms. By default, the initial workspace contains the lifetimes
   matrix and its date labels.

5. Topologically sort the graph constructed in (1) to produce an
   execution order for any terms that were not pre-populated.  This
   logic lives in TermGraph.

6. Iterate over the terms in the order computed in (5). For each term:

   a. Fetch the term's inputs from the workspace, possibly removing
      unneeded leading rows from the input (see ExecutionPlan.offset
      for details on why we might have extra leading rows).

   b. Call ``term._compute`` with the inputs. Store the results into
      the workspace.

   c. Decrement "reference counts" on the term's inputs, and remove
      their results from the workspace if the refcount hits 0. This
      significantly reduces the maximum amount of memory that we
      consume during execution

   This logic lives in SimplePipelineEngine.compute_chunk.

7. Extract the pipeline's outputs from the workspace and convert them
   into "narrow" format, with output labels dictated by the Pipeline's
   screen. This logic lives in SimplePipelineEngine._to_narrow.
"""

from ziplime.pipeline.engine.pipeline_engine import PipelineEngine  # noqa: F401
from ziplime.pipeline.engine.no_engine_registered import NoEngineRegistered  # noqa: F401
from ziplime.pipeline.engine.exploding_pipeline_engine import (  # noqa: F401
    ExplodingPipelineEngine,
)
from ziplime.pipeline.engine.simple_pipeline_engine import SimplePipelineEngine  # noqa: F401
from ziplime.pipeline.engine_utils import (  # noqa: F401
    default_populate_initial_workspace,
)


__all__ = [
    'PipelineEngine',
    'NoEngineRegistered',
    'ExplodingPipelineEngine',
    'SimplePipelineEngine',
    'default_populate_initial_workspace',
]
