"""Support for hub."""

import structlog


_logger = structlog.get_logger(__name__)


FLOATING_REVISIONS = frozenset({"main", "master", "refs/heads/main", "refs/heads/master", None})
