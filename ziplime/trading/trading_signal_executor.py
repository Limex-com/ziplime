from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.exchanges.exchange import Exchange
from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_signals.order_execute_trading_signal import OrderExecuteTradingSignal


class TradingSignalExecutor:
    def __init__(self):
        self.running_trading_signals = []
        self.completed_trading_signals = []

    async def create_order_execute_trading_signal(self,
                                                  algorithm: BaseTradingAlgorithm,
                                                  order: OrderRequest, exchange: Exchange):
        trading_signal = OrderExecuteTradingSignal(order=order, exchange=exchange, algorithm=algorithm)
        self.running_trading_signals.append(trading_signal)
        await trading_signal.start()
        return trading_signal