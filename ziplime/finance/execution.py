#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
import datetime
import uuid
from sys import float_info
from numpy import isfinite
import ziplime.utils.math_utils as zp_math
from ziplime.assets.entities.asset import Asset
from ziplime.errors import BadOrderParameters
from ziplime.trading.entities.orders.market_order_request import MarketOrderRequest
from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType
from ziplime.utils.compat import consistent_round


class ExecutionStyle(metaclass=abc.ABCMeta):
    """Base class for order execution styles."""

    _exchange = None

    @abc.abstractmethod
    def get_limit_price(self, is_buy):
        """
        Get the limit price for this order.
        Returns either None or a numerical value >= 0.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_stop_price(self, is_buy):
        """
        Get the stop price for this order.
        Returns either None or a numerical value >= 0.
        """
        raise NotImplementedError

    @property
    def exchange(self):
        """
        The exchange to which this order should be routed.
        """
        return self._exchange

    def to_order_type(self) -> OrderType:
        raise NotImplementedError("to_order_type not implemented for ExecutionStyle")

    async def to_order_request(self, base_asset: Asset, quote_asset: Asset,
                               quantity: int,
                               exchange_name: str,
                               creation_dt: datetime.datetime,
                               ): ...


class MarketOrder(ExecutionStyle):
    """
    Execution style for orders to be filled at current market price.

    This is the default for orders placed with :func:`~ziplime.api.order`.
    """

    def __init__(self, exchange=None):
        self._exchange = exchange

    def get_limit_price(self, is_buy: bool):
        return None

    def get_stop_price(self, is_buy: bool):
        return None

    def to_order_type(self) -> OrderType:
        return OrderType.MARKET

    async def to_order_request(self, base_asset: Asset, quote_asset: Asset,
                               quantity: int,
                               exchange_name: str,
                               creation_dt: datetime.datetime,
                               ) -> MarketOrderRequest:
        order_req = MarketOrderRequest(
            order_id=uuid.uuid4().hex,
            trading_pair=TradingPair(base_asset=base_asset,
                                     quote_asset=quote_asset,
                                     exchange_name=exchange_name),
            order_side=OrderSide.BUY if quantity > 0 else OrderSide.SELL,
            quantity=float(quantity),
            exchange_name=exchange_name,
            creation_date=creation_dt
        )
        return order_req

    def __str__(self):
        return "MarketOrder()"


class LimitOrder(ExecutionStyle):
    """
    Execution style for orders to be filled at a price equal to or better than
    a specified limit price.

    Parameters
    ----------
    limit_price : float
        Maximum price for buys, or minimum price for sells, at which the order
        should be filled.
    """

    def __init__(self, limit_price, asset=None, exchange=None):
        check_stoplimit_prices(price=limit_price, label="limit")

        self.limit_price = limit_price
        self._exchange = exchange
        self.asset = asset

    def get_limit_price(self, is_buy):
        return asymmetric_round_price(
            self.limit_price,
            is_buy,
            tick_size=(0.01 if self.asset is None else self.asset.tick_size),
        )

    def get_stop_price(self, is_buy):
        return None

    def to_order_type(self) -> OrderType:
        return OrderType.LIMIT

    def __str__(self):
        return f"LimitOrder(limit_price={self.limit_price})"


class StopOrder(ExecutionStyle):
    """
    Execution style representing a market order to be placed if market price
    reaches a threshold.

    Parameters
    ----------
    stop_price : float
        Price threshold at which the order should be placed. For sells, the
        order will be placed if market price falls below this value. For buys,
        the order will be placed if market price rises above this value.
    """

    def __init__(self, stop_price, asset=None, exchange=None):
        check_stoplimit_prices(price=stop_price, label="stop")

        self.stop_price = stop_price
        self._exchange = exchange
        self.asset = asset

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, is_buy):
        return asymmetric_round_price(
            self.stop_price,
            not is_buy,
            tick_size=(0.01 if self.asset is None else self.asset.tick_size),
        )

    def to_order_type(self) -> OrderType:
        return OrderType.STOP

    def __str__(self):
        return f"StopOrder(stop_price={self.stop_price})"


class StopLimitOrder(ExecutionStyle):
    """
    Execution style representing a limit order to be placed if market price
    reaches a threshold.

    Parameters
    ----------
    limit_price : float
        Maximum price for buys, or minimum price for sells, at which the order
        should be filled, if placed.
    stop_price : float
        Price threshold at which the order should be placed. For sells, the
        order will be placed if market price falls below this value. For buys,
        the order will be placed if market price rises above this value.
    """

    def __init__(self, limit_price, stop_price, asset=None, exchange=None):
        check_stoplimit_prices(price=limit_price, label="limit")
        check_stoplimit_prices(price=stop_price, label="stop")

        self.limit_price = limit_price
        self.stop_price = stop_price
        self._exchange = exchange
        self.asset = asset

    def get_limit_price(self, is_buy):
        return asymmetric_round_price(
            self.limit_price,
            is_buy,
            tick_size=(0.01 if self.asset is None else self.asset.tick_size),
        )

    def get_stop_price(self, is_buy):
        return asymmetric_round_price(
            self.stop_price,
            not is_buy,
            tick_size=(0.01 if self.asset is None else self.asset.tick_size),
        )

    def to_order_type(self) -> OrderType:
        return OrderType.STOP_LIMIT

    def __str__(self):
        return f"StopLimitOrder(limit_price={self.limit_price}, stop_price={self.stop_price})"


def asymmetric_round_price(price: float, prefer_round_down: bool, tick_size: float, diff: float = 0.95):
    """
    Asymmetric rounding function for adjusting prices to the specified number
    of places in a way that "improves" the price. For limit prices, this means
    preferring to round down on buys and preferring to round up on sells.
    For stop prices, it means the reverse.

    If prefer_round_down == True:
        When .05 below to .95 above a specified decimal place, use it.
    If prefer_round_down == False:
        When .95 below to .05 above a specified decimal place, use it.

    In math-speak:
    If prefer_round_down: [<X-1>.0095, X.0195) -> round to X.01.
    If not prefer_round_down: (<X-1>.0005, X.0105] -> round to X.01.
    """
    precision = zp_math.number_of_decimal_places(tick_size)
    multiplier = int(tick_size * (10 ** precision))
    diff -= 0.5  # shift the difference down
    diff *= 10.0 ** -precision  # adjust diff to precision of tick size
    diff *= multiplier  # adjust diff to value of tick_size

    # Subtracting an epsilon from diff to enforce the open-ness of the upper
    # bound on buys and the lower bound on sells.  Using the actual system
    # epsilon doesn't quite get there, so use a slightly less epsilon-ey value.
    epsilon = float_info.epsilon * 10
    diff = diff - epsilon

    # relies on rounding half away from zero, unlike numpy's bankers' rounding
    rounded = tick_size * consistent_round(
        (price - (diff if prefer_round_down else -diff)) / tick_size
    )
    if zp_math.tolerant_equals(rounded, 0.0):
        return 0.0
    return rounded


def check_stoplimit_prices(price: float, label: str):
    """
    Check to make sure the stop/limit prices are reasonable and raise
    a BadOrderParameters exception if not.
    """
    try:
        if not isfinite(float(price)):
            raise BadOrderParameters(
                msg=f"Attempted to place an order with a {label} price "
                    f"of {price}."
            )
    # This catches arbitrary objects
    except TypeError as exc:
        raise BadOrderParameters(
            msg=f"Attempted to place an order with a {label} price "
                f"of {type(price)}."
        ) from exc

    if price < 0:
        raise BadOrderParameters(
            msg=f"Can't place a {label} order with a negative price."
        )
