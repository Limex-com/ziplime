from ziplime.finance.controls.trading_control import TradingControl


class AssetDateBounds(TradingControl):
    """TradingControl representing a prohibition against ordering an asset before
    its start_date, or after its end_date.
    """

    def __init__(self, on_error):
        super(AssetDateBounds, self).__init__(on_error)

    def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """Fail if the algo has passed this Asset's end_date, or before the
        Asset's start date.
        """
        # If the order is for 0 shares, then silently pass through.
        if amount == 0:
            return

        normalized_algo_dt = algo_datetime.normalize().tz_localize(None)

        # Fail if the algo is before this Asset's start_date
        if asset.start_date:
            normalized_start = asset.start_date.normalize()
            if normalized_algo_dt < normalized_start:
                metadata = {"asset_start_date": normalized_start}
                self.handle_violation(asset, amount, algo_datetime, metadata=metadata)
        # Fail if the algo has passed this Asset's end_date
        if asset.end_date:
            normalized_end = asset.end_date.normalize()
            if normalized_algo_dt > normalized_end:
                metadata = {"asset_end_date": normalized_end}
                self.handle_violation(asset, amount, algo_datetime, metadata=metadata)
