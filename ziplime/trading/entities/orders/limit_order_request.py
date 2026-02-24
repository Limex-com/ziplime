from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType


class LimitOrderRequest(OrderRequest):

    def __init__(self, order_id: str, trading_pair: TradingPair,
                 order_side: OrderSide,
                 quantity: float, limit_price: float):
        super().__init__(order_id=order_id, order_type=OrderType.LIMIT)
        self.order_side = order_side
        self.quantity = quantity
        self.limit_price = limit_price
        self.trading_pair=trading_pair
