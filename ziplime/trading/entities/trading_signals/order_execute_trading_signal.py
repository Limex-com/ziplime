from ziplime.core.entities.enums.task_status import TaskStatus
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.order_status import OrderStatus
from ziplime.finance.execution import MarketOrder, LimitOrder
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.trading.entities.orders.market_order_request import MarketOrderRequest
from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_signals.trading_signal import TradingSignal
from ziplime.trading.enums.order_type import OrderType


class OrderExecuteTradingSignal(TradingSignal):

    # order request should be replaced with OrderExecuteTradingSignalConfig ->
    def __init__(self, algorithm: BaseTradingAlgorithm, order: OrderRequest, exchange: Exchange):
        super().__init__(algorithm=algorithm, exchange=exchange)
        self.order = order

    def get_orders_to_execute(self) -> list[OrderRequest]:
        return [self.order]

    async def on_refresh_interval(self):
        """
        This method is responsible for controlling the task based on the status of the executor.

        :return: None
        """
        if self.status != TaskStatus.RUNNING:
            return None


        # Define variables needed before the match statement
        exchange = self.exchange

        match self.order.order_type:
            case OrderType.MARKET:
                execution_style = MarketOrder()
            case OrderType.LIMIT:
                execution_style = LimitOrder(limit_price=self.order.limit_price)
            case _:  # Default case to handle other order types
                # For now, default to market order if type is not recognized
                raise ValueError(f"Unsupported order type: {self.order.order_type}")

        order = Order(
            dt=self.algorithm.simulation_dt,
            asset=self.order.trading_pair.base_asset,
            amount=self.order.quantity,
            id=self.order.order_id,
            commission=0.00,
            filled=0,
            execution_style=execution_style,
            status=OrderStatus.OPEN,
            exchange_name=exchange.name
        )

        submitted_order = await self.algorithm.exchanges[self.order.exchange_name].submit_order(order)
        self.stop()
        self.algorithm.new_order_submitted(order=submitted_order)
        # getting price
        # self.algorithm.exchanges["xx"].get_price("")
