from enum import StrEnum


class CashEventKind(StrEnum):
    """Classification of an explicit native-currency cash posting.

    Attributes
    ----------
    OPENING
        Initial cash balance.
    EXTERNAL
        Contribution or withdrawal excluded from investment PNL.
    FX
        One cash leg of an actual foreign-exchange execution.

    Notes
    -----
    String values preserve serialized history compatibility. Parse stored values
    with ``CashEventKind(value)``; unsupported values raise ``ValueError``.
    """

    OPENING = "opening"
    EXTERNAL = "external"
    FX = "fx"
