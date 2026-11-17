import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.portfolio import Portfolio
from ziplime.finance.controls.trading_control import TradingControl
from ziplime.domain.bar_data import BarData


class MaxOrderCount(TradingControl):
    """TradingControl representing a limit on the number of orders that can be
    placed in a given trading day.
    """

    def __init__(self, on_error: str, max_count: int) -> None:

        super(MaxOrderCount, self).__init__(on_error, max_count=max_count)
        self.orders_placed = 0
        self.max_count = max_count
        self.current_date = None

    async def validate(self, asset: ExchangeAsset, amount: int, portfolio: Portfolio,
                       algo_datetime: datetime.datetime, algo_current_data: BarData) -> None:
        """Fail if we've already placed self.max_count orders today."""
        algo_date = algo_datetime.date()

        # Reset order count if it's a new day.
        if self.current_date and self.current_date != algo_date:
            self.orders_placed = 0
        self.current_date = algo_date

        if self.orders_placed >= self.max_count:
            self.handle_violation(asset, amount, algo_datetime)
        self.orders_placed += 1
