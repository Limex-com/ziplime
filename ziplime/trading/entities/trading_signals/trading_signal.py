from abc import abstractmethod
from logging import Logger

import structlog

from ziplime.core.base_task import BaseTask

from ziplime.exchanges.exchange import Exchange
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.trading.entities.orders.order_request import OrderRequest


class TradingSignal(BaseTask):
    """Trading signal represents action that needs to be done on the market.
    It can produce one or more orders depending on the signal type
    """

    def __init__(self, algorithm: BaseTradingAlgorithm, exchange: Exchange,
                 logger: Logger = structlog.get_logger(__name__)):
        super().__init__(logger=logger)
        self.algorithm = algorithm
        self.exchange = exchange

    @abstractmethod
    def get_orders_to_execute(self) -> list[OrderRequest]: ...
