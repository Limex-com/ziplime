"""Loadable term specialization for execution plans."""

from ziplime.pipeline import LoadableTerm
from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.domain import Domain


def maybe_specialize(term: Term, domain: Domain) -> Term:
    """Specialize a term if it's loadable."""
    if isinstance(term, LoadableTerm):
        return term.specialize(domain)
    return term
