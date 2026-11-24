"""Support for user_datasets."""

import re
import structlog


_logger = structlog.get_logger(__name__)


SCHEME = "dataset://"


NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


DEFAULT_MAX_BYTES = 1024 * 1024 * 1024


DEFAULT_CACHE_BYTES = 512 * 1024 * 1024
