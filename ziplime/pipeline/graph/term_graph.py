"""Term graph for graph."""

from collections.abc import Iterable
from collections.abc import Mapping
import networkx as nx
import numpy as np
from ziplime.lib.adjusted_array import AdjustedArray
from ziplime.pipeline import LoadableTerm
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.graph.cyclic_dependency import CyclicDependency
from ziplime.pipeline.graph.support import SCREEN_NAME


class TermGraph:
    """
    An abstract representation of Pipeline Term dependencies.

    This class does not keep any additional metadata about any term relations
    other than dependency ordering.  As such it is only useful in contexts
    where you care exclusively about order properties (for example, when
    drawing visualizations of execution order).

    Parameters
    ----------
    terms : dict
        A dict mapping names to final output terms.

    Attributes
    ----------
    outputs

    Methods
    -------
    ordered()
        Return a topologically-sorted iterator over the terms in self.
    execution_order(workspace, refcounts)
        Return a topologically-sorted iterator over the terms in self, skipping
        entries in ``workspace`` and entries with refcounts of zero.

    See Also
    --------
    ExecutionPlan
    """

    def __init__(self, terms: dict[str, Term]) -> None:
        self.graph = nx.DiGraph()

        self._frozen = False
        parents = set()
        for term in terms.values():
            self._add_to_graph(term, parents)
            # No parents should be left between top-level terms.
            assert not parents

        self._outputs = terms

        # Mark that no more terms should be added to the graph.
        self._frozen = True

    def __contains__(self, term: Term):
        return term in self.graph

    def _add_to_graph(self, term: Term, parents: set[Term]) -> None:
        """
        Add a term and all its children to ``graph``.

        ``parents`` is the set of all the parents of ``term` that we've added
        so far. It is only used to detect dependency cycles.
        """
        if self._frozen:
            raise ValueError(
                "Can't mutate %s after construction." % type(self).__name__
            )

        # If we've seen this node already as a parent of the current traversal,
        # it means we have an unsatisifiable dependency.  This should only be
        # possible if the term's inputs are mutated after construction.
        if term in parents:
            raise CyclicDependency(term)

        parents.add(term)

        self.graph.add_node(term)

        for dependency in term.dependencies:
            self._add_to_graph(dependency, parents)
            self.graph.add_edge(dependency, term)

        parents.remove(term)

    @property
    def outputs(self):
        """
        Dict mapping names to designated output terms.
        """
        return self._outputs

    @property
    def screen_name(self):
        """Name of the specially-designated ``screen`` term for the pipeline."""
        return SCREEN_NAME

    def execution_order(self, workspace: Mapping[Term, np.ndarray | AdjustedArray], refcounts: dict[Term, int]):
        """
        Return a topologically-sorted list of the terms in ``self`` which
        need to be computed.

        Filters out any terms that are already present in ``workspace``, as
        well as any terms with refcounts of 0.

        Parameters
        ----------
        workspace : dict[Term, np.ndarray]
            Initial state of workspace for a pipeline execution. May contain
            pre-computed values provided by ``populate_initial_workspace``.
        refcounts : dict[Term, int]
            Reference counts for terms to be computed. Terms with reference
            counts of 0 do not need to be computed.
        """
        return list(
            nx.topological_sort(
                self.graph.subgraph(
                    {
                        term
                        for term, refcount in refcounts.items()
                        if refcount > 0 and term not in workspace
                    },
                ),
            )
        )

    def ordered(self):
        return iter(nx.topological_sort(self.graph))

    # @lazyval
    @property
    def loadable_terms(self):
        return {term for term in self.graph if isinstance(term, LoadableTerm)}

    # @lazyval
    @property
    def jpeg(self):
        from ziplime.pipeline.visualize import display_graph

        return display_graph(self, "jpeg")

    # @lazyval
    @property
    def png(self):
        from ziplime.pipeline.visualize import display_graph

        return display_graph(self, "png")

    # @lazyval
    @property
    def svg(self):
        from ziplime.pipeline.visualize import display_graph

        return display_graph(self, "svg")

    def _repr_png_(self):
        return self.png.data

    def initial_refcounts(self, initial_terms: Iterable[Term]):
        """
        Calculate initial refcounts for execution of this graph.

        Parameters
        ----------
        initial_terms : iterable[Term]
            An iterable of terms that were pre-computed before graph execution.

        Each node starts with a refcount equal to its outdegree, and output
        nodes get one extra reference to ensure that they're still in the graph
        at the end of execution.
        """
        refcounts = dict(self.graph.out_degree())
        for t in self.outputs.values():
            refcounts[t] += 1

        for t in initial_terms:
            self._decref_dependencies_recursive(t, refcounts, set())

        return refcounts

    def _decref_dependencies_recursive(self, term: Term, refcounts: dict[Term, int], garbage: set[Term]) -> None:
        """
        Decrement terms recursively.

        Notes
        -----
        This should only be used to build the initial workspace, after that we
        should use:
        :meth:`~ziplime.pipeline.graph.TermGraph.decref_dependencies`
        """
        # Edges are tuple of (from, to).
        for parent, _ in self.graph.in_edges([term]):
            refcounts[parent] -= 1
            # No one else depends on this term. Remove it from the
            # workspace to conserve memory.
            if refcounts[parent] == 0:
                garbage.add(parent)
                self._decref_dependencies_recursive(parent, refcounts, garbage)

    def decref_dependencies(self, term: Term, refcounts: dict[Term, int]):
        """
        Decrement in-edges for ``term`` after computation.

        Parameters
        ----------
        term : ziplime.pipeline.Term
            The term whose parents should be decref'ed.
        refcounts : dict[Term -> int]
            Dictionary of refcounts.

        Return
        ------
        garbage : set[Term]
            Terms whose refcounts hit zero after decrefing.
        """
        garbage = set()
        # Edges are tuple of (from, to).
        for parent, _ in self.graph.in_edges([term]):
            refcounts[parent] -= 1
            # No one else depends on this term. Remove it from the
            # workspace to conserve memory.
            if refcounts[parent] == 0:
                garbage.add(parent)
        return garbage

    def __len__(self) -> int:
        return len(self.graph)
