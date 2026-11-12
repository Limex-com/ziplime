import datetime
from dataclasses import dataclass, field

from ziplime.finance.domain.order_valuation import OrderValuation


@dataclass(frozen=True)
class OrderValuations:
    """Immutable common-currency estimates in the caller's requested order.

    Attributes
    ----------
    currency : str
        ISO code used by every monetary value and aggregate property.
    valuation_at : datetime.datetime
        Aware timestamp shared by the batch.
    orders : tuple[OrderValuation, ...]
        Estimates in input order, without deduplicating repeated orders.
        These constructor values do not represent reserved buying power.
    gross_value : float
        Sum of absolute order values; buys and sells do not offset.
    net_value : float
        Signed sum of order values; buys and sells offset.

    Notes
    -----
    Totals are calculated together once during initialization, not passed to the
    constructor. They remain frozen with the rest of the instance. Replacing a
    batch with ``dataclasses.replace`` recalculates its totals for the new orders.
    """

    currency: str
    valuation_at: datetime.datetime
    orders: tuple[OrderValuation, ...]

    gross_value: float = field(init=False, compare=False)
    net_value: float = field(init=False, compare=False)

    def __post_init__(self) -> None:
        """Calculate both immutable totals once, preserving the original summation."""
        object.__setattr__(self, "gross_value", sum(abs(order.monetary.value) for order in self.orders))
        object.__setattr__(self, "net_value", sum(order.monetary.value for order in self.orders))
