from enum import StrEnum


class InvestmentEventKind(StrEnum):
    """Classification of an immutable native investment posting.

    Attributes
    ----------
    TRADE
        Executed fill with signed quantity and native unit consideration.
    FEE
        Fee or rebate linked to a trade in the same position key.
    INCOME
        Investment income or an income entitlement awaiting payment.
    SPLIT
        Share split expressed as an old-shares/new-shares ratio.

    Notes
    -----
    String values preserve serialized history compatibility. Parse stored values
    with ``InvestmentEventKind(value)``; unsupported values raise ``ValueError``.
    """

    TRADE = "trade"
    FEE = "fee"
    INCOME = "income"
    SPLIT = "split"
