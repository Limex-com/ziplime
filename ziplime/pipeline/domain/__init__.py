"""
This module defines the interface and implementations of Pipeline domains.

A domain represents a set of labels for the arrays computed by a Pipeline.
Currently, this means that a domain defines two things:

1. A calendar defining the dates to which the pipeline's inputs and outputs
   should be aligned. The calendar is represented concretely by a pandas
   DatetimeIndex.

2. The set of assets that the pipeline should compute over. Right now, the only
   supported way of representing this set is with a two-character country code
   describing the country of assets over which the pipeline should compute. In
   the future, we expect to expand this functionality to include more general
   concepts.
"""

from collections.abc import Callable  # noqa: F401
from ziplime.pipeline.domain.base import Domain  # noqa: F401
from ziplime.pipeline.domain.generic import GenericDomain  # noqa: F401
from ziplime.pipeline.domain.generic import GENERIC  # noqa: F401
from ziplime.pipeline.domain.equity_calendar_domain import EquityCalendarDomain  # noqa: F401
from ziplime.pipeline.domain.errors import AmbiguousDomain  # noqa: F401
from ziplime.pipeline.domain.equity_session_domain import EquitySessionDomain  # noqa: F401


__all__ = [
    'Domain',
    'GenericDomain',
    'GENERIC',
    'EquityCalendarDomain',
    'AmbiguousDomain',
    'EquitySessionDomain',
]


def __getattr__(name: str) -> Callable:
    # Preserve the public import without making domain definitions depend on Term.
    if name == "infer_domain":
        from ziplime.pipeline.domain_inference import infer_domain

        globals()[name] = infer_domain
        return infer_domain
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | {"infer_domain"})
