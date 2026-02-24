import datetime

from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType


class MarketOrderRequest(OrderRequest):

    def __init__(self, order_id: str, trading_pair: TradingPair, order_side: OrderSide, quantity: float,
                 exchange_name: str, creation_date: datetime.datetime
                 ):
        super().__init__(order_id=order_id, trading_pair=trading_pair, exchange_name=exchange_name,
                         creation_date=exchange_name, quantity=quantity,
                         order_type=OrderType.MARKET)
        self.order_side = order_side
        self.quantity = quantity
        self.trading_pair = trading_pair
