"""Style for api."""


class Style:
    """MarketOrder / LimitOrder / StopOrder / StopLimitOrder whose prices may be arrays."""

    def __init__(self, limit_price=None, stop_price=None) -> None:
        self.limit_price, self.stop_price = limit_price, stop_price
