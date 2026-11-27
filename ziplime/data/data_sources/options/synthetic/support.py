"""Support for synthetic."""

import structlog


_logger = structlog.get_logger(__name__)


SYNTHETIC_DATA_WARNING = (
    "SYNTHETIC OPTION PRICES: generated from a volatility model, not observed in any market. "
    "Valid for developing and testing option machinery; NOT valid for estimating a strategy's "
    "historical return, Sharpe ratio or drawdown."
)
