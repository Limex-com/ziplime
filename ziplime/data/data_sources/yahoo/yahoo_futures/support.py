"""Support for yahoo_futures."""

import structlog


_logger = structlog.get_logger(__name__)


CONTINUOUS_SUFFIX = "=F"


MONTH_CODES = "FGHJKMNQUVXZ"


MONTH_OF_CODE = {code: index + 1 for index, code in enumerate(MONTH_CODES)}


QUARTERLY = "HMUZ"


MONTHLY = MONTH_CODES


GRAIN = "HKNUZ"
