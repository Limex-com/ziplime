from ziplime.finance.controls.trading_control import TradingControl


class RestrictedListOrder(TradingControl):
    """TradingControl representing a restricted list of assets that
    cannot be ordered by the algorithm.

    Parameters
    ----------
    restrictions : ziplime.finance.asset_restrictions.Restrictions
        Object representing restrictions of a group of assets.
    """

    def __init__(self, on_error, restrictions):
        super(RestrictedListOrder, self).__init__(on_error)
        self.restrictions = restrictions

    def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """
        Fail if the asset is in the restricted_list.
        """
        if self.restrictions.is_restricted(asset, algo_datetime):
            self.handle_violation(asset, amount, algo_datetime)
