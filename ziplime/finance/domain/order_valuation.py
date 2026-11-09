from dataclasses import dataclass
from ziplime.finance.domain.monetary_valuation import MonetaryValuation
from ziplime.finance.domain.order_price_basis import OrderPriceBasis
from ziplime.finance.domain.order_quantity_basis import OrderQuantityBasis


@dataclass(frozen=True)
class OrderValuation:
    """Immutable estimate for one order; fields are constructor arguments.

    Attributes
    ----------
    order_id : str
        Identity of the submitted order.
    exchange_name : str
        Execution exchange, not necessarily the listing's market MIC.
    trading_account_id : str
        Account to which the order belongs.
    quantity_basis : OrderQuantityBasis
        Full original signed quantity or signed quantity minus filled quantity.
    price_basis : OrderPriceBasis
        Current market quote or the order's explicit limit quote.
    monetary : MonetaryValuation
        Native and converted estimate, with current price/FX provenance.
        Neither basis reconstructs actual historical fill consideration.
    """

    order_id: str
    exchange_name: str
    trading_account_id: str
    quantity_basis: OrderQuantityBasis
    price_basis: OrderPriceBasis
    monetary: MonetaryValuation
