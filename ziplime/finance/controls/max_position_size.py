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

    async def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """Fail if the given order would cause the magnitude of our position to be
        greater in shares than self.max_shares or greater in dollar value than
        self.max_notional.
        """

        if self.asset is not None and self.asset != asset:
            return

        # `Portfolio.positions` in this fork is nested exchange -> account -> asset, not the flat
        # `{asset: Position}` upstream had, so subscripting it by asset raised `KeyError` for every
        # instrument -- including, and especially, the ones with no position yet.
        current_share_count = await portfolio.get_asset_positions_amount(asset=asset)
        shares_post_order = current_share_count + amount

        too_many_shares = (
                self.max_shares is not None and abs(shares_post_order) > self.max_shares
        )
        if too_many_shares:
            self.handle_violation(asset, amount, algo_datetime)

        # Only fetch a price when there is a notional cap to check it against. This read happened
        # on every order regardless, so a share-only cap paid for a quote it never used.
        if self.max_notional is None:
            return

        current_price = await algo_current_data.current(asset, "price")
        if abs(shares_post_order * current_price) > self.max_notional:
            self.handle_violation(asset, amount, algo_datetime)
