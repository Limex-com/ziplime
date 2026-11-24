"""Progress model for progress."""

import datetime
import time
from ziplime.pipeline import Term
from ziplime.pipeline.terms.computable_term import ComputableTerm
from ziplime.pipeline.terms.loadable_term import LoadableTerm


class ProgressModel:
    """
    Model object for tracking progress of a Pipeline execution.

    Parameters
    ----------
    nterms : int
        Number of terms in the execution plan of the Pipeline being run.
    start_date : pd.Timestamp
        Start date of the range over which ``plan`` will be computed.
    end_date : pd.Timestamp
        End date of the range over which ``plan`` will be computed.

    Methods
    -------
    start_chunk(start_date, end_date)
    finish_chunk(start_date, end_date)
    load_precomputed_terms(terms)
    start_load_terms(terms)
    finish_load_terms(terms)
    start_compute_term(term)
    finish_compute_term(term)
    finish(success)

    Attributes
    ----------
    state : {'init', 'loading', 'computing', 'error', 'success'}
        Current state of the execution.
    percent_complete : float
        Percent of execution that has been completed, on a scale from 0 to 100.
    execution_time : float
        Number of seconds that the execution required. Only available if state
        is 'error' or 'success'.
    execution_bounds : (pd.Timestamp, pd.Timestamp)
        Pair of (start_date, end_date) for the entire execution.
    current_chunk_bounds : (pd.Timestamp, pd.Timestamp)
        Pair of (start_date, end_date) for the currently executing chunk.
    current_work : [ziplime.pipeline.Term]
        List of terms currently being loaded or computed.
    """

    def __init__(self, start_date: datetime.date, end_date: datetime.date) -> None:
        self._start_date = start_date
        self._end_date = end_date

        # +1 to be inclusive of end_date.
        self._total_days = (end_date - start_date).days + 1
        self._progress = 0.0
        self._days_completed = 0

        self._state = "init"

        # Number of days in current chunk.
        self._current_chunk_size = None

        # (start_date, end_date) of current chunk.
        self._current_chunk_bounds = None

        # How much should we increment progress by after completing a term?
        self._completed_term_increment = None

        # How much should we increment progress by after completing a chunk?
        # This is zero unless we compute a pipeline with no terms, in which
        # case it will be the full chunk percentage.
        self._completed_chunk_increment = None

        # Terms currently being computed.
        self._current_work = None

        # Tracking state for total elapsed time.
        self._start_time = time.time()
        self._end_time = None

    # These properties form the interface for Publishers.
    @property
    def state(self):
        return self._state

    @property
    def percent_complete(self):
        return round(self._progress * 100.0, 3)

    @property
    def execution_time(self):
        if self._end_time is None:
            raise ValueError("Can't get execution_time until execution is complete.")
        return self._end_time - self._start_time

    @property
    def execution_bounds(self):
        return (self._start_date, self._end_date)

    @property
    def current_chunk_bounds(self):
        return self._current_chunk_bounds

    @property
    def current_work(self):
        return self._current_work

    # These methods form the interface for ProgressHooks.
    def start_chunk(self, terms: list[Term], start_date: datetime.date, end_date: datetime.date) -> None:
        days_since_start = (end_date - self._start_date).days + 1
        self._current_chunk_size = days_since_start - self._days_completed
        self._current_chunk_bounds = (start_date, end_date)

        # What percent of our overall progress will happen in this chunk?
        chunk_percent = float(self._current_chunk_size) / self._total_days

        # How much of that is associated with each completed term?
        nterms = len(terms)
        if nterms:
            self._completed_term_increment = chunk_percent / len(terms)
            self._completed_chunk_increment = 0.0
        else:
            # Special case. If we don't have any terms, increment the entire
            # chunk's worth of progress when we finish the chunk.
            self._completed_term_increment = 0.0
            self._completed_chunk_increment = chunk_percent

    def finish_chunk(self, terms: list[Term], start_date: datetime.date, end_date: datetime.date) -> None:
        self._days_completed += self._current_chunk_size
        self._progress += self._completed_chunk_increment

    def start_load_terms(self, terms: list[LoadableTerm]) -> None:
        self._state = "loading"
        self._current_work = terms

    def finish_load_terms(self, terms: list[LoadableTerm]) -> None:
        self._finish_terms(nterms=len(terms))

    def start_compute_term(self, term: ComputableTerm) -> None:
        self._state = "computing"
        self._current_work = [term]

    def finish_compute_term(self, term: ComputableTerm) -> None:
        self._finish_terms(nterms=1)

    def finish(self, success: bool) -> None:
        self._end_time = time.time()
        if success:
            self._state = "success"
        else:
            self._state = "error"

    def _finish_terms(self, nterms: int) -> None:
        self._progress += nterms * self._completed_term_increment
