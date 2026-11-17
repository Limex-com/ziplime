import datetime as datetime

from ziplime.utils.compat import contextmanager

from ziplime.pipeline.hooks.iface import PipelineHooks
from ziplime.pipeline.pipeline import Pipeline
from ziplime.pipeline.terms.computable_term import ComputableTerm
from ziplime.pipeline.terms.loadable_term import LoadableTerm
from ziplime.pipeline.terms.term import Term


class NoHooks(PipelineHooks):
    """A PipelineHooks that defines no-op methods for all available hooks."""

    @contextmanager
    def running_pipeline(self, pipeline: 'Pipeline', start_date: 'datetime.date', end_date: 'datetime.date'):
        yield

    @contextmanager
    def computing_chunk(self, terms: 'list[Term]', start_date: 'datetime.date', end_date: 'datetime.date'):
        yield

    @contextmanager
    def loading_terms(self, terms: 'list[LoadableTerm]'):
        yield

    @contextmanager
    def computing_term(self, term: 'ComputableTerm'):
        yield
