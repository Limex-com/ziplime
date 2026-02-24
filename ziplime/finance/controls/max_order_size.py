from ziplime.finance.controls.trading_control import TradingControl


class MaxOrderSize(TradingControl):
    """TradingControl representing a limit on the magnitude of any single order
    placed with the given asset.  Can be specified by share or by dollar
    value.
    """

    def __init__(self, on_error, asset=None, max_shares=None, max_notional=None):
        super(MaxOrderSize, self).__init__(
            on_error, asset=asset, max_shares=max_shares, max_notional=max_notional
        )
        self.asset = asset
        self.max_shares = max_shares
        self.max_notional = max_notional

        if max_shares is None and max_notional is None:
            raise ValueError("Must supply at least one of max_shares and max_notional")

        if max_shares and max_shares < 0:
            raise ValueError("max_shares cannot be negative.")

        if max_notional and max_notional < 0:
            raise ValueError("max_notional must be positive.")

    def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """Fail if the magnitude of the given order exceeds either self.max_shares
        or self.max_notional.
        """

        if self.asset is not None and self.asset != asset:
            return

        if self.max_shares is not None and abs(amount) > self.max_shares:
            self.handle_violation(asset, amount, algo_datetime)

        current_asset_price = algo_current_data.current(asset, "price")
        order_value = amount * current_asset_price

        too_much_value = (
                self.max_notional is not None and abs(order_value) > self.max_notional
        )

        if too_much_value:
            self.handle_violation(asset, amount, algo_datetime)
