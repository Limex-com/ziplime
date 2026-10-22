from dataclasses import dataclass


@dataclass
class InvestmentLot:
    """Mutable FIFO projection state in one reporting currency.

    Attributes
    ----------
    transaction_id : str
        Opening trade identity, used to associate later acquisition fees.
    quantity : float
        Signed remaining units: positive long, negative short; adjusted by splits.
    cost : float
        Signed remaining historical consideration in the projection currency,
        including allocated opening fees. A short normally starts with negative
        cost representing its opening proceeds. Partial closes reduce it pro rata.
    """

    transaction_id: str
    quantity: float
    cost: float
