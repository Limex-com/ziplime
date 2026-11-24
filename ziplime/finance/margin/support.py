"""Support for margin."""

import structlog


_logger = structlog.get_logger(__name__)


NO_MARGIN_MODEL_WARNING = "NO FUTURES MARGIN MODEL"


DEFAULT_MARGIN_CURRENCY = "EUR"
