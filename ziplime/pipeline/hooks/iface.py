import datetime
from ziplime.utils.compat import contextmanager as _contextmanager
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.pipeline import Pipeline
# Keep track of which methods of PipelineHooks are contextmanagers. Used by
# DelegatingHooks to properly delegate to sub-hooks.
PIPELINE_HOOKS_CONTEXT_MANAGERS = set()


def contextmanager(f):
    """
    Wrapper for contextlib.contextmanager that tracks which methods of
    PipelineHooks are contextmanagers in CONTEXT_MANAGER_METHODS.
    """
    PIPELINE_HOOKS_CONTEXT_MANAGERS.add(f.__name__)
    return _contextmanager(f)


class PipelineHooks:
    """
    Interface for instrumenting SimplePipelineEngine executions.

    Methods with names like 'on_event()' should be normal methods. They will be
    called by the engine after the corresponding event.

    Methods with names like 'doing_thing()' should be context managers. They
    will be entered by the engine around the corresponding event.

    Methods
    -------
    running_pipeline(self, pipeline, start_date, end_date, chunked)
    computing_chunk(self, terms, start_date, end_date)
    loading_terms(self, terms)
    computing_term(self, term):
    """

    @contextmanager
    def running_pipeline(self, pipeline: Pipeline, start_date: datetime.date, end_date: datetime.date):
        """
        Contextmanager entered during execution of run_pipeline or
        run_chunked_pipeline.

        Parameters
        ----------
        pipeline : ziplime.pipeline.Pipeline
            The pipeline being executed.
        start_date : pd.Timestamp
            First date of the execution.
        end_date : pd.Timestamp
            Last date of the execution.
        """

    @contextmanager
    def computing_chunk(self, terms: list[Term], start_date: datetime.date, end_date: datetime.date):
        """
        Contextmanager entered during execution of compute_chunk.

        Parameters
        ----------
        terms : list[ziplime.pipeline.Term]
            List of terms, in execution order, that will be computed. This
            value may change between chunks if ``populate_initial_workspace``
            prepopulates different terms at different times.
        start_date : pd.Timestamp
            First date of the chunk.
        end_date : pd.Timestamp
            Last date of the chunk.
        """

    @contextmanager
    def loading_terms(self, terms):
        """Contextmanager entered when loading a batch of LoadableTerms.

        Parameters
        ----------
        terms : list[ziplime.pipeline.LoadableTerm]
            Terms being loaded.
        """

    @contextmanager
    def computing_term(self, term):
        """Contextmanager entered when computing a ComputableTerm.

        Parameters
        ----------
        terms : ziplime.pipeline.ComputableTerm
            Terms being computed.
        """
