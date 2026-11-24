"""Execution style for execution."""

import abc
import datetime
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.trading.enums.order_type import OrderType


class ExecutionStyle(metaclass=abc.ABCMeta):
    """Base class for order execution styles."""

    @abc.abstractmethod
    def get_limit_price(self, is_buy: bool) -> float | None:
        """
        Get the limit price for this order.
        Returns either None or a numerical value >= 0.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_stop_price(self, is_buy: bool) -> float | None:
        """
        Get the stop price for this order.
        Returns either None or a numerical value >= 0.
        """
        raise NotImplementedError

    def to_order_type(self) -> OrderType:
        raise NotImplementedError("to_order_type not implemented for ExecutionStyle")

    async def to_order_request(self, base_asset: ExchangeAsset, quote_asset: ExchangeAsset,
                               quantity: int,
                               creation_dt: datetime.datetime,
                               ): ...
