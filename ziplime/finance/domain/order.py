import math

from ziplime.assets.entities.asset import Asset
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import ExecutionStyle
from ziplime.protocol import DataSourceType

SELL = 1 << 0
BUY = 1 << 1
STOP = 1 << 2
LIMIT = 1 << 3


# ORDER_FIELDS_TO_IGNORE = {"type", "direction", "_status", "asset"}


class Order:
    def __init__(
            self,
            id,
            dt,
            asset: Asset,
            amount: int,
            filled: int,
            commission: float,
            execution_style: ExecutionStyle,
            status: OrderStatus,
            exchange_name: str,
            exchange_order_id: str = None
    ):
        """
        @dt - datetime.datetime that the order was placed
        @asset - asset for the order.
        @amount - the number of shares to buy/sell
                  a positive sign indicates a buy
                  a negative sign indicates a sell
        @filled - how many shares of the order have been filled so far
        """

        # get a string representation of the uuid.
        self.id = id
        self.dt = dt
        self.reason = None
        self.created = dt
        self.asset = asset
        self.amount = amount
        self.filled = filled
        self.commission = commission
        self.exchange_name = exchange_name
        self._status = status

        is_buy = amount > 0
        self.stop = execution_style.get_stop_price(is_buy=is_buy)
        self.limit = execution_style.get_limit_price(is_buy=is_buy)
        self.stop_reached = False
        self.limit_reached = False
        self.direction = math.copysign(1, self.amount)
        self.type = DataSourceType.ORDER
        self.execution_style = execution_style
        self.exchange_order_id = exchange_order_id

    def to_dict(self):
        dct = {
            "amount": self.amount,
            "commission": self.commission,
            "created": self.created,
            "dt": self.dt,
            "exchange_order_id": self.exchange_order_id,
            "filled": self.filled,
            "id": self.id,
            "limit": self.limit,
            "limit_reached": self.limit_reached,
            "reason": self.reason,
            "stop": self.stop,
            "stop_reached": self.stop_reached,
            "status": self.status
        }
        return dct

    def check_triggers(self, price, dt):
        """
        Update internal state based on price triggers and the
        trade event's price.
        """
        (
            stop_reached,
            limit_reached,
            sl_stop_reached,
        ) = self.check_order_triggers(price)
        if (stop_reached, limit_reached) != (
                self.stop_reached,
                self.limit_reached,
        ):
            self.dt = dt
        self.stop_reached = stop_reached
        self.limit_reached = limit_reached
        if sl_stop_reached:
            # Change the STOP LIMIT order into a LIMIT order
            self.stop = None

    def get_order_type(self) -> int:
        order_type = 0

        if self.amount > 0:
            order_type |= BUY
        else:
            order_type |= SELL

        if self.stop is not None:
            order_type |= STOP

        if self.limit is not None:
            order_type |= LIMIT
        return order_type

    # TODO: simplify
    # flake8: noqa: C901
    def check_order_triggers(self, current_price):
        """
        Given an order and a trade event, return a tuple of
        (stop_reached, limit_reached).
        For market orders, will return (False, False).
        For stop orders, limit_reached will always be False.
        For limit orders, stop_reached will always be False.
        For stop limit orders a Boolean is returned to flag
        that the stop has been reached.

        Orders that have been triggered already (price targets reached),
        the order's current values are returned.
        """
        if self.triggered:
            return self.stop_reached, self.limit_reached, False

        stop_reached = False
        limit_reached = False
        sl_stop_reached = False
        order_type = self.get_order_type()
        if order_type == BUY | STOP | LIMIT:
            if current_price >= self.stop:
                sl_stop_reached = True
                if current_price <= self.limit:
                    limit_reached = True
        elif order_type == SELL | STOP | LIMIT:
            if current_price <= self.stop:
                sl_stop_reached = True
                if current_price >= self.limit:
                    limit_reached = True
        elif order_type == BUY | STOP:
            if current_price >= self.stop:
                stop_reached = True
        elif order_type == SELL | STOP:
            if current_price <= self.stop:
                stop_reached = True
        elif order_type == BUY | LIMIT:
            if current_price <= self.limit:
                limit_reached = True
        elif order_type == SELL | LIMIT:
            # This is a SELL LIMIT order
            if current_price >= self.limit:
                limit_reached = True

        return stop_reached, limit_reached, sl_stop_reached

    def handle_split(self, ratio):
        # update the amount, limit_price, and stop_price
        # by the split's ratio

        # info here: http://finra.complinet.com/en/display/display_plain.html?
        # rbid=2403&element_id=8950&record_id=12208&print=1

        # new_share_amount = old_share_amount / ratio
        # new_price = old_price * ratio

        self.amount = int(self.amount / ratio)

        if self.limit is not None:
            self.limit = round(self.limit * ratio, 2)

        if self.stop is not None:
            self.stop = round(self.stop * ratio, 2)

    @property
    def status(self):
        if not self.open_amount:
            return OrderStatus.FILLED
        elif self._status == OrderStatus.HELD and self.filled:
            return OrderStatus.OPEN
        else:
            return self._status

    @status.setter
    def status(self, status):
        self._status = status

    def cancel(self):
        self.status = OrderStatus.CANCELLED

    def reject(self, reason: str = ""):
        self.status = OrderStatus.REJECTED
        self.reason = reason

    def hold(self, reason: str = ""):
        self.status = OrderStatus.HELD
        self.reason = reason

    @property
    def open(self):
        return self.status in [OrderStatus.OPEN, OrderStatus.HELD]

    @property
    def triggered(self) -> bool:
        """
        For a market order, True.
        For a stop order, True IFF stop_reached.
        For a limit order, True IFF limit_reached.
        """
        if self.stop is not None and not self.stop_reached:
            return False

        if self.limit is not None and not self.limit_reached:
            return False

        return True

    @property
    def open_amount(self):
        return self.amount - self.filled

    def __repr__(self):
        """
        String representation for this object.
        """
        return "Order(%s)" % self.to_dict().__repr__()
