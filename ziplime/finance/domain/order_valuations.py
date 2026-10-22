import datetime
from dataclasses import dataclass

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
    """

    currency: str
    valuation_at: datetime.datetime
    orders: tuple[OrderValuation, ...]

    @property
    def gross_value(self) -> float:
        """Return the sum of absolute order values; buys and sells do not offset."""
        return sum(abs(order.monetary.value) for order in self.orders)

    @property
    def net_value(self) -> float:
        """Return the signed sum of order values; buys and sells offset."""
        return sum(order.monetary.value for order in self.orders)
