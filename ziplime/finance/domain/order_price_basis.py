from enum import StrEnum


class OrderPriceBasis(StrEnum):
    """Price source for a current order-value estimate.

    Attributes
    ----------
    MARKET
        Use the current available market close and its availability timestamp.
    LIMIT
        Use the order's explicit limit quote; orders without a limit are rejected.

    Notes
    -----
    String values remain compatible with serialization and can be parsed with
    ``OrderPriceBasis(value)``; invalid values raise ``ValueError``.
    """

    MARKET = "market"
    LIMIT = "limit"
