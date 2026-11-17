import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.domain.portfolio import Portfolio
from ziplime.finance.asset_restrictions import Restrictions
from ziplime.finance.controls.trading_control import TradingControl
from ziplime.domain.bar_data import BarData


class RestrictedListOrder(TradingControl):
    """TradingControl representing a restricted list of assets that
    cannot be ordered by the algorithm.

    Parameters
    ----------
    restrictions : ziplime.finance.asset_restrictions.Restrictions
        Object representing restrictions of a group of assets.
    """

    def __init__(self, on_error: str, restrictions: Restrictions) -> None:
        super(RestrictedListOrder, self).__init__(on_error)
        self.restrictions = restrictions

    async def validate(self, asset: ExchangeAsset, amount: int, portfolio: Portfolio,
                       algo_datetime: 'datetime.datetime', algo_current_data: 'BarData') -> None:
        """
        Fail if the asset is in the restricted_list.
        """
        if self.restrictions.is_restricted(asset, algo_datetime):
            self.handle_violation(asset, amount, algo_datetime)
