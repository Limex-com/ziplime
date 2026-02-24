from ziplime.finance.controls.trading_control import TradingControl


class MaxPositionSize(TradingControl):
    """TradingControl representing a limit on the maximum position size that can
    be held by an algo for a given asset.
    """

    def __init__(self, on_error, asset=None, max_shares=None, max_notional=None):
        super(MaxPositionSize, self).__init__(
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
        """Fail if the given order would cause the magnitude of our position to be
        greater in shares than self.max_shares or greater in dollar value than
        self.max_notional.
        """

        if self.asset is not None and self.asset != asset:
            return

        current_share_count = portfolio.positions[asset].amount
        shares_post_order = current_share_count + amount

        too_many_shares = (
                self.max_shares is not None and abs(shares_post_order) > self.max_shares
        )
        if too_many_shares:
            self.handle_violation(asset, amount, algo_datetime)

        current_price = algo_current_data.current(asset, "price")
        value_post_order = shares_post_order * current_price

        too_much_value = (
                self.max_notional is not None and abs(value_post_order) > self.max_notional
        )

        if too_much_value:
            self.handle_violation(asset, amount, algo_datetime)
