"""Market order for execution."""

from ziplime.trading.enums.order_type import OrderType
from ziplime.finance.execution.execution_style import ExecutionStyle


class MarketOrder(ExecutionStyle):
    """
    Execution style for orders to be filled at current market price.

    This is the default for orders placed with :func:`~ziplime.api.order`.
    """

    def __init__(self, same_day_execution: bool = True) -> None:
        self.same_day_execution = same_day_execution

    def get_limit_price(self, is_buy: bool) -> None:
        return None

    def get_stop_price(self, is_buy: bool) -> None:
        return None

    def to_order_type(self) -> OrderType:
        return OrderType.MARKET

    def __str__(self) -> str:
        return "MarketOrder()"
