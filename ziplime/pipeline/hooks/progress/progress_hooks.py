"""Progress hooks for progress."""

import datetime
from collections.abc import Callable
from ziplime.utils.compat import contextmanager
from ziplime.pipeline.hooks.iface import PipelineHooks
from ziplime.pipeline import Term
from ziplime.pipeline.pipeline import Pipeline
from ziplime.pipeline.hooks.progress.ipython_widget_progress_publisher import IPythonWidgetProgressPublisher
from ziplime.pipeline.hooks.progress.progress_model import ProgressModel


class ProgressHooks(PipelineHooks):
    """
    Hooks implementation for displaying progress.

    Parameters
    ----------
    publisher_factory : callable
        Function producing a new object with a ``publish()`` method that takes
        a ``ProgressModel`` and publishes progress to a consumer.
    """

    def __init__(self, publisher_factory: Callable) -> None:
        self._publisher_factory = publisher_factory
        self._reset_transient_state()

    def _reset_transient_state(self) -> None:
        self._start_date = None
        self._end_date = None
        self._model = None
        self._publisher = None

    @classmethod
    def with_widget_publisher(cls):
        """
        Construct a ProgressHooks that publishes to Jupyter via
        ``IPython.display``.
        """
        return cls(publisher_factory=IPythonWidgetProgressPublisher)

    @classmethod
    def with_static_publisher(cls, publisher):
        """Construct a ProgressHooks that uses an already-constructed publisher."""
        return cls(publisher_factory=lambda: publisher)

    def _publish(self) -> None:
        self._publisher.publish(self._model)

    @contextmanager
    def running_pipeline(self, pipeline: Pipeline, start_date: datetime.date, end_date: datetime.date):
        self._start_date = start_date
        self._end_date = end_date

        try:
            yield
        except Exception:
            if self._model is None:
                # This will only happen if an error happens in the Pipeline
                # Engine beteween entering `running_pipeline` and the first
                # `computing_chunk` call. If that happens, just propagate the
                # exception.
                raise
            self._model.finish(success=False)
            self._publish()
            raise
        else:
            self._model.finish(success=True)
            self._publish()
        finally:
            self._reset_transient_state()

    @contextmanager
    def computing_chunk(self, terms: list[Term], start_date: datetime.date, end_date: datetime.date):
        # Set up model on first compute_chunk call.
        if self._model is None:
            self._publisher = self._publisher_factory()
            self._model = ProgressModel(
                start_date=self._start_date,
                end_date=self._end_date,
            )

        try:
            self._model.start_chunk(terms=terms, start_date=start_date, end_date=end_date)
            self._publish()
            yield
        finally:
            self._model.finish_chunk(terms=terms, start_date=start_date, end_date=end_date)
            self._publish()

    @contextmanager
    def loading_terms(self, terms: list[Term]):
        try:
            self._model.start_load_terms(terms=terms)
            self._publish()
            yield
        finally:
            self._model.finish_load_terms(terms=terms)
            self._publish()

    @contextmanager
    def computing_term(self, term: Term):
        try:
            self._model.start_compute_term(term=term)
            self._publish()
            yield
        finally:
            self._model.finish_compute_term(term=term)
            self._publish()
