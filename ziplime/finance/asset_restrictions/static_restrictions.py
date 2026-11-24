"""Static restrictions for asset_restrictions."""

import pandas as pd
from collections.abc import Iterable
import datetime
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.utils.numpy_utils import vectorized_is_element
from ziplime.finance.restriction_utils import _is_one_asset
from ziplime.finance.asset_restrictions.restrictions import Restrictions


class StaticRestrictions(Restrictions):
    """Static restrictions stored in memory that are constant regardless of dt
    for each asset.

    Parameters
    ----------
    restricted_list : iterable of assets
        The assets to be restricted
    """

    def __init__(self, restricted_list: Iterable[Asset | ExchangeAsset]) -> None:
        self._restricted_set = frozenset(restricted_list)

    def is_restricted(self, assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset], dt: datetime.datetime):
        """An asset is restricted for all dts if it is in the static list."""
        if _is_one_asset(assets):
            return assets in self._restricted_set
        return pd.Series(
            index=pd.Index(assets),
            data=vectorized_is_element(assets, self._restricted_set),
        )
