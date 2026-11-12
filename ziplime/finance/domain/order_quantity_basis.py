from enum import StrEnum


class OrderQuantityBasis(StrEnum):
    """Signed order quantity to include in a current monetary estimate.

    Attributes
    ----------
    SUBMITTED
        Use the full original quantity, including already-filled units.
    REMAINING
        Use original quantity minus signed filled quantity.

    Notes
    -----
    Neither choice freezes prices or FX at submission time. String values can be
    parsed with ``OrderQuantityBasis(value)``; invalid values raise ``ValueError``.
    """

    SUBMITTED = "submitted"
    REMAINING = "remaining"
