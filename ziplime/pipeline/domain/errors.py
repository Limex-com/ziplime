"""Errors for domain."""

from textwrap import dedent
from ziplime.utils.formatting import bulleted_list
from ziplime.pipeline.domain.base import Domain


class AmbiguousDomain(Exception):
    """Raised when we attempt to infer a domain from a collection of mixed terms."""

    _TEMPLATE = dedent(
        """\
        Found terms with conflicting domains:
        {domains}"""
    )

    def __init__(self, domains: list[Domain]) -> None:
        self.domains = domains

    def __str__(self):
        return self._TEMPLATE.format(
            domains=bulleted_list(self.domains, indent=2),
        )
