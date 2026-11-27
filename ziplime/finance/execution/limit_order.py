"""Limit order for execution."""

from ziplime.trading.enums.order_type import OrderType
from ziplime.finance.execution.execution_style import ExecutionStyle
from ziplime.finance.execution.utils import asymmetric_round_price
from ziplime.finance.execution.utils import check_stoplimit_prices


class LimitOrder(ExecutionStyle):
    """
    Execution style for orders to be filled at a price equal to or better than
    a specified limit price.

    Parameters
    ----------
    limit_price : float
        Maximum price for buys, or minimum price for sells, at which the order
        should be filled.
    """

    def __init__(self, limit_price: float, tick_size: float = 0.01) -> None:
        check_stoplimit_prices(price=limit_price, label="limit")

        self.limit_price = limit_price
        self.tick_size = tick_size

    def get_limit_price(self, is_buy: bool) -> float:
        return asymmetric_round_price(
            self.limit_price,
            is_buy,
            tick_size=self.tick_size,
        )

    def get_stop_price(self, is_buy: bool) -> None:
        return None

    def to_order_type(self) -> OrderType:
        return OrderType.LIMIT

    def __str__(self) -> str:
        return f"LimitOrder(limit_price={self.limit_price})"
