"""Infer a pipeline domain from its input terms."""

from collections.abc import Iterable

from ziplime.pipeline.domain import AmbiguousDomain, Domain, GENERIC
from ziplime.pipeline.terms.term import Term


def infer_domain(terms: Iterable[Term]) -> Domain:
    """Infer the domain from a collection of terms.

    Generic terms do not constrain the domain. Empty inputs and entirely
    generic inputs produce GENERIC. A single concrete domain is returned;
    conflicting concrete domains raise AmbiguousDomain.
    """
    domains = {t.domain for t in terms}
    num_domains = len(domains)

    if num_domains == 0:
        return GENERIC
    elif num_domains == 1:
        return domains.pop()
    elif num_domains == 2 and GENERIC in domains:
        domains.remove(GENERIC)
        return domains.pop()
    else:
        # Generic terms do not contribute to a domain conflict.
        domains.discard(GENERIC)
        raise AmbiguousDomain(sorted(domains, key=repr))
