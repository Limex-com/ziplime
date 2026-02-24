from ziplime.finance.controls.trading_control import TradingControl


class LongOnly(TradingControl):
    """TradingControl representing a prohibition against holding short positions."""

    def __init__(self, on_error):
        super(LongOnly, self).__init__(on_error)

    def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """
        Fail if we would hold negative shares of asset after completing this
        order.
        """
        if portfolio.positions[asset].amount + amount < 0:
            self.handle_violation(asset, amount, algo_datetime)
