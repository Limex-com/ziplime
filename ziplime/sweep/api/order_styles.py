"""Order styles for api."""

from ziplime.sweep.api.style import Style


def MarketOrder(*args, **kwargs) -> Style:
    return Style()


def LimitOrder(limit_price, *args, **kwargs) -> Style:
    return Style(limit_price=limit_price)


def StopOrder(stop_price, *args, **kwargs) -> Style:
    return Style(stop_price=stop_price)


def StopLimitOrder(limit_price, stop_price, *args, **kwargs) -> Style:
    return Style(limit_price=limit_price, stop_price=stop_price)


STYLES = {"MarketOrder": MarketOrder, "LimitOrder": LimitOrder, "StopOrder": StopOrder,
          "StopLimitOrder": StopLimitOrder}
