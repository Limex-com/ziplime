import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.portfolio import Portfolio
from ziplime.finance.controls.trading_control import TradingControl
from ziplime.domain.bar_data import BarData


class LongOnly(TradingControl):
    """TradingControl representing a prohibition against holding short positions."""

    def __init__(self, on_error: str) -> None:
        super(LongOnly, self).__init__(on_error)

    async def validate(self, asset: ExchangeAsset, amount: int, portfolio: Portfolio,
                       algo_datetime: datetime.datetime, algo_current_data: BarData) -> None:
        """
        Fail if we would hold negative shares of asset after completing this
        order.
        """
        # See `MaxPositionSize.validate`: this fork's `Portfolio.positions` is nested, so the flat
        # subscript upstream used raises `KeyError` rather than reporting a zero position.
        held = await portfolio.get_asset_positions_amount(asset=asset)
        if held + amount < 0:
            self.handle_violation(asset, amount, algo_datetime)
