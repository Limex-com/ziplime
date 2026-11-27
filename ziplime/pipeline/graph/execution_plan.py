"""Execution plan for graph."""

import datetime
import networkx as nx
import pandas as pd
from ziplime.lib.adjusted_array import AdjustedArray
from ziplime.pipeline import LoadableTerm
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.domain import Domain
from ziplime.pipeline.graph_utils import maybe_specialize
from ziplime.pipeline.graph.term_graph import TermGraph


class ExecutionPlan(TermGraph):
    """
    Graph representation of Pipeline Term dependencies that includes metadata
    about extra rows required to perform computations.

    Each node in the graph has an `extra_rows` attribute, indicating how many,
    if any, extra rows we should compute for the node. Extra rows are most
    often needed when a term is an input to a rolling window computation. For
    example, if we compute a 30 day moving average of price from day X to day
    Y, we need to load price data for the range from day (X - 29) to day Y.

    Parameters
    ----------
    domain : ziplime.pipeline.domain.Domain
        The domain of execution for which we need to build a plan.
    terms : dict
        A dict mapping names to final output terms.
    start_date : pd.Timestamp
        The first date for which output is requested for ``terms``.
    end_date : pd.Timestamp
        The last date for which output is requested for ``terms``.

    Attributes
    ----------
    domain
    extra_rows
    outputs
    offset
    """

    def __init__(
        self,
        domain: Domain,
        terms: list[Term],
        start_date: datetime.date,
        end_date: datetime.date,
        min_extra_rows: int = 0,
    ) -> None:
        super(ExecutionPlan, self).__init__(terms=terms)

        # Specialize all the LoadableTerms in the graph to our domain, so that
        # when the engine requests an execution order, we emit the specialized
        # versions of loadable terms.
        #
        # NOTE: We're explicitly avoiding using self.loadable_terms here.
        #
        # At this point the graph still contains un-specialized loadable terms,
        # and this is where we're actually going through and specializing all
        # of them. We don't want use self.loadable_terms because it's a
        # lazyval, and we don't want its result to be cached until after we've
        # specialized.
        specializations = {
            t: t.specialize(domain=domain) for t in self.graph if isinstance(t, LoadableTerm)
        }
        self.graph = nx.relabel.relabel_nodes(self.graph, specializations)

        self.domain = domain

        sessions = domain.sessions()
        for term in terms.values():
            self.set_extra_rows(
                term=term,
                all_dates=sessions,
                start_date=start_date,
                end_date=end_date,
                min_extra_rows=min_extra_rows,
            )

        self._assert_all_loadable_terms_specialized_to(domain=domain)

    def set_extra_rows(
        self,
        term: Term,
        all_dates: pd.DatetimeIndex,
        start_date: datetime.date,
        end_date: datetime.date,
        min_extra_rows: int,
    ) -> None:
        # Specialize any loadable terms before adding extra rows.
        term = maybe_specialize(term=term, domain=self.domain)

        # A term can require that additional extra rows beyond the minimum be
        # computed.  This is most often used with downsampled terms, which need
        # to ensure that the first date is a computation date.
        extra_rows_for_term = term.compute_extra_rows(
            all_dates=all_dates,
            start_date=start_date,
            end_date=end_date,
            min_extra_rows=min_extra_rows,
        )
        if extra_rows_for_term < min_extra_rows:
            raise ValueError(
                "term %s requested fewer rows than the minimum of %d"
                % (
                    term,
                    min_extra_rows,
                )
            )

        self._ensure_extra_rows(term=term, N=extra_rows_for_term)

        for dependency, additional_extra_rows in term.dependencies.items():
            self.set_extra_rows(
                term=dependency,
                all_dates=all_dates,
                start_date=start_date,
                end_date=end_date,
                min_extra_rows=extra_rows_for_term + additional_extra_rows,
            )

    # @lazyval
    @property
    def offset(self) -> dict[tuple[Term, Term], int]:
        """
        For all pairs (term, input) such that `input` is an input to `term`,
        compute a mapping::

            (term, input) -> offset(term, input)

        where ``offset(term, input)`` is the number of rows that ``term``
        should truncate off the raw array produced for ``input`` before using
        it. We compute this value as follows::

            offset(term, input) = (extra_rows_computed(input)
                                   - extra_rows_computed(term)
                                   - requested_extra_rows(term, input))
        Examples
        --------

        Case 1
        ~~~~~~

        Factor A needs 5 extra rows of USEquityPricing.close, and Factor B
        needs 3 extra rows of the same.  Factor A also requires 5 extra rows of
        USEquityPricing.high, which no other Factor uses.  We don't require any
        extra rows of Factor A or Factor B

        We load 5 extra rows of both `price` and `high` to ensure we can
        service Factor A, and the following offsets get computed::

            offset[Factor A, USEquityPricing.close] == (5 - 0) - 5 == 0
            offset[Factor A, USEquityPricing.high]  == (5 - 0) - 5 == 0
            offset[Factor B, USEquityPricing.close] == (5 - 0) - 3 == 2
            offset[Factor B, USEquityPricing.high] raises KeyError.

        Case 2
        ~~~~~~

        Factor A needs 5 extra rows of USEquityPricing.close, and Factor B
        needs 3 extra rows of Factor A, and Factor B needs 2 extra rows of
        USEquityPricing.close.

        We load 8 extra rows of USEquityPricing.close (enough to load 5 extra
        rows of Factor A), and the following offsets get computed::

            offset[Factor A, USEquityPricing.close] == (8 - 3) - 5 == 0
            offset[Factor B, USEquityPricing.close] == (8 - 0) - 2 == 6
            offset[Factor B, Factor A]              == (3 - 0) - 3 == 0

        Notes
        -----
        `offset(term, input) >= 0` for all valid pairs, since `input` must be
        an input to `term` if the pair appears in the mapping.

        This value is useful because we load enough rows of each input to serve
        all possible dependencies.  However, for any given dependency, we only
        want to compute using the actual number of required extra rows for that
        dependency.  We can do so by truncating off the first `offset` rows of
        the loaded data for `input`.

        See Also
        --------
        :meth:`ziplime.pipeline.graph.ExecutionPlan.offset`
        :meth:`ziplime.pipeline.engine.ExecutionPlan.mask_and_dates_for_term`
        :meth:`ziplime.pipeline.engine.SimplePipelineEngine._inputs_for_term`
        """
        extra = self.extra_rows

        out = {}
        for term in self.graph:
            for dep, requested_extra_rows in term.dependencies.items():
                specialized_dep = maybe_specialize(term=dep, domain=self.domain)

                # How much bigger is the result for dep compared to term?
                size_difference = extra[specialized_dep] - extra[term]

                # Subtract the portion of that difference that was required by
                # term's lookback window.
                offset = size_difference - requested_extra_rows
                out[term, specialized_dep] = offset

        return out

    # @lazyval
    @property
    def extra_rows(self) -> dict[Term, int]:
        """
        A dict mapping `term` -> `# of extra rows to load/compute of `term`.

        Notes
        ----
        This value depends on the other terms in the graph that require `term`
        **as an input**.  This is not to be confused with `term.dependencies`,
        which describes how many additional rows of `term`'s inputs we need to
        load, and which is determined entirely by `Term` itself.

        Examples
        --------
        Our graph contains the following terms:

            A = SimpleMovingAverage([USEquityPricing.high], window_length=5)
            B = SimpleMovingAverage([USEquityPricing.high], window_length=10)
            C = SimpleMovingAverage([USEquityPricing.low], window_length=8)

        To compute N rows of A, we need N + 4 extra rows of `high`.
        To compute N rows of B, we need N + 9 extra rows of `high`.
        To compute N rows of C, we need N + 7 extra rows of `low`.

        We store the following extra_row requirements:

        self.extra_rows[high] = 9  # Ensures that we can service B.
        self.extra_rows[low] = 7

        See Also
        --------
        :meth:`ziplime.pipeline.graph.ExecutionPlan.offset`
        :meth:`ziplime.pipeline.Term.dependencies`
        """

        return {term: self.graph.nodes[term]["extra_rows"] for term in self.graph.nodes}

    def _ensure_extra_rows(self, term: Term, N: int) -> None:
        """
        Ensure that we're going to compute at least N extra rows of `term`.
        """
        attrs = dict(self.graph.nodes())[term]
        attrs["extra_rows"] = max(N, attrs.get("extra_rows", 0))

    def mask_and_dates_for_term(
        self, term: Term, root_mask_term: Term, workspace: dict[Term, AdjustedArray], all_dates: pd.DatetimeIndex
    ):
        """
        Load mask and mask row labels for term.

        Parameters
        ----------
        term : Term
            The term to load the mask and labels for.
        root_mask_term : Term
            The term that represents the root asset exists mask.
        workspace : dict[Term, any]
            The values that have been computed for each term.
        all_dates : pd.DatetimeIndex
            All of the dates that are being computed for in the pipeline.

        Returns
        -------
        mask : np.ndarray
            The correct mask for this term.
        dates : np.ndarray
            The slice of dates for this term.
        """
        mask = term.mask
        mask_offset = self.extra_rows[mask] - self.extra_rows[term]

        # This offset is computed against root_mask_term because that is what
        # determines the shape of the top-level dates array.
        dates_offset = self.extra_rows[root_mask_term] - self.extra_rows[term]

        return workspace[mask][mask_offset:], all_dates[dates_offset:]

    def _assert_all_loadable_terms_specialized_to(self, domain: Domain) -> None:
        """Make sure that we've specialized all loadable terms in the graph."""
        for term in self.graph.nodes():
            if isinstance(term, LoadableTerm):
                assert term.domain is domain
