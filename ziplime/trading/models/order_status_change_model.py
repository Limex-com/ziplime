import datetime
from ziplime.finance.domain.order_status import OrderStatus


class OrderStatusChange:
    def __init__(self, order_id: str, status: 'OrderStatus', timestamp: 'datetime.datetime') -> None:
        self.order_id = order_id
        self.status = status
        self.timestamp = timestamp
