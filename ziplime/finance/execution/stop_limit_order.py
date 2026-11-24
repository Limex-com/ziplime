"""Stop limit order for execution."""

from ziplime.trading.enums.order_type import OrderType
from ziplime.finance.execution.execution_style import ExecutionStyle
from ziplime.finance.execution.utils import asymmetric_round_price
from ziplime.finance.execution.utils import check_stoplimit_prices


class StopLimitOrder(ExecutionStyle):
    """
    Execution style representing a limit order to be placed if market price
    reaches a threshold.

    Parameters
    ----------
    limit_price : float
        Maximum price for buys, or minimum price for sells, at which the order
        should be filled, if placed.
    stop_price : float
        Price threshold at which the order should be placed. For sells, the
        order will be placed if market price falls below this value. For buys,
        the order will be placed if market price rises above this value.
    """

    def __init__(self, limit_price: float, stop_price: float, tick_size: float = 0.01) -> None:
        check_stoplimit_prices(price=limit_price, label="limit")
        check_stoplimit_prices(price=stop_price, label="stop")

        self.limit_price = limit_price
        self.stop_price = stop_price
        self.tick_size = tick_size

    def get_limit_price(self, is_buy: bool) -> float:
        return asymmetric_round_price(
            self.limit_price,
            is_buy,
            tick_size=self.tick_size,
        )

    def get_stop_price(self, is_buy: bool) -> float:
        return asymmetric_round_price(
            self.stop_price,
            not is_buy,
            tick_size=self.tick_size,
        )

    def to_order_type(self) -> OrderType:
        return OrderType.STOP_LIMIT

    def __str__(self) -> str:
        return f"StopLimitOrder(limit_price={self.limit_price}, stop_price={self.stop_price})"
