from ziplime.finance.controls.trading_control import TradingControl


class LongOnly(TradingControl):
    """TradingControl representing a prohibition against holding short positions."""

    def __init__(self, on_error):
        super(LongOnly, self).__init__(on_error)

    async def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """
        Fail if we would hold negative shares of asset after completing this
        order.
        """
        current_share_count = await portfolio.get_exchange_asset_positions_amount(asset=asset)
        if current_share_count + amount < 0:
            self.handle_violation(asset, amount, algo_datetime)
