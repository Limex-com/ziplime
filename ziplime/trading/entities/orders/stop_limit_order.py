from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType


class StopLimitOrder(OrderRequest):

    def __init__(self, order_id: str, order_side: OrderSide, quantity: float, limit_price: float,
               stop_price: float):
        super().__init__(order_id=order_id, order_type=OrderType.LIMIT)
        self.order_side = order_side
        self.quantity = quantity
        self.limit_price = limit_price
        self.stop_price = stop_price
