"""No restrictions for asset_restrictions."""

import pandas as pd
from collections.abc import Iterable
import datetime
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.restriction_utils import _is_one_asset
from ziplime.finance.asset_restrictions.restrictions import Restrictions


class NoRestrictions(Restrictions):
    """A no-op restrictions that contains no restrictions."""

    def is_restricted(self, assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset], dt: datetime.datetime):
        if _is_one_asset(assets):
            return False
        return pd.Series(index=pd.Index(assets), data=False)
