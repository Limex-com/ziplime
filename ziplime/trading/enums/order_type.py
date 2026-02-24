from enum import Enum


class OrderType(Enum):
    """
    Enum representing all possible order types in trading systems.
    """
    # Basic order types
    MARKET = "MARKET"  # Execute immediately at current market price
    LIMIT = "LIMIT"  # Buy/sell at a specified price or better
    LIMIT_MAKER = "LIMIT_MAKER"  # Like limit but ensures the order is a maker

    # Additional order types
    STOP = "STOP"  # Market order that activates at a specified trigger price
    STOP_LIMIT = "STOP_LIMIT"  # Limit order that activates at a specified trigger price
    TRAILING_STOP = "TRAILING_STOP"  # Stop order where trigger price adjusts with market movement
    FILL_OR_KILL = "FILL_OR_KILL"  # Must be filled entirely immediately or canceled entirely
    IMMEDIATE_OR_CANCEL = "IMMEDIATE_OR_CANCEL"  # Must be filled immediately (partial fill allowed) or canceled
    GOOD_TILL_CANCELLED = "GOOD_TILL_CANCELLED"  # Order stays active until manually canceled
    GOOD_TILL_DATE = "GOOD_TILL_DATE"  # Order stays active until a specified date/time
    ICEBERG = "ICEBERG"  # Large order that shows only a small portion to the market
    ONE_CANCELS_OTHER = "ONE_CANCELS_OTHER"  # Two linked orders where execution of one cancels the other
    POST_ONLY = "POST_ONLY"  # Order is posted to the book and not matched immediately
    BRACKET = "BRACKET"  # Set of orders including entry, take profit, and stop loss
    CONDITIONAL = "CONDITIONAL"  # Order that executes based on specified market conditions
    PEGGED = "PEGGED"  # Order that adjusts price relative to a reference price
    REDUCE_ONLY = "REDUCE_ONLY"  # Order that can only reduce, not increase, a position
    TAKE_PROFIT = "TAKE_PROFIT"  # Order to close a position at a profit target
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"  # Limit order version of take profit

    def is_limit_type(self) -> bool:
        """
        Determines if the order type is a limit-based order.
        """
        return self in (OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.STOP_LIMIT,
                        OrderType.TAKE_PROFIT_LIMIT, OrderType.POST_ONLY)

    def is_stop_type(self) -> bool:
        """
        Determines if the order type is a stop-based order.
        """
        return self in (OrderType.STOP, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP)

    def requires_price(self) -> bool:
        """
        Determines if the order type requires a price parameter.
        """
        return self.is_limit_type() or self.is_stop_type()
