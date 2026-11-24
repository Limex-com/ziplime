"""Historical restrictions for asset_restrictions."""

from numpy import vectorize
from functools import partial
import pandas as pd
from toolz import groupby
from collections.abc import Iterable
import datetime
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.restriction_utils import _is_one_asset
from ziplime.finance.asset_restrictions.support import RESTRICTION_STATES
from ziplime.finance.asset_restrictions.support import Restriction
from ziplime.finance.asset_restrictions.restrictions import Restrictions


class HistoricalRestrictions(Restrictions):
    """Historical restrictions stored in memory with effective dates for each
    asset.

    Parameters
    ----------
    restrictions : iterable of namedtuple Restriction
        The restrictions, each defined by an asset, effective date and state
    """

    def __init__(self, restrictions: Iterable[Restriction]) -> None:
        # A dict mapping each asset to its restrictions, which are sorted by
        # ascending order of effective_date
        self._restrictions_by_asset = {
            asset: sorted(restrictions_for_asset, key=lambda x: x.effective_date)
            for asset, restrictions_for_asset in groupby(
                lambda x: x.asset, restrictions
            ).items()
        }

    def is_restricted(self, assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset], dt: datetime.datetime):
        """Returns whether or not an asset or iterable of assets is restricted
        on a dt.
        """
        if _is_one_asset(assets):
            return self._is_restricted_for_asset(assets, dt)

        is_restricted = partial(self._is_restricted_for_asset, dt=dt)
        return pd.Series(
            index=pd.Index(assets),
            data=vectorize(is_restricted, otypes=[bool])(assets),
        )

    def _is_restricted_for_asset(self, asset: Asset | ExchangeAsset, dt: datetime.datetime):
        state = RESTRICTION_STATES.ALLOWED
        for r in self._restrictions_by_asset.get(asset, ()):
            r_effective_date = r.effective_date
            if r_effective_date.tzinfo is None:
                r_effective_date = r_effective_date.tz_localize(dt.tzinfo)
            if r_effective_date > dt:
                break
            state = r.state
        return state == RESTRICTION_STATES.FROZEN
