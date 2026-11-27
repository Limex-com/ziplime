"""Restrictions for asset_restrictions."""

import abc
from collections.abc import Iterable
import datetime
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset


class Restrictions(metaclass=abc.ABCMeta):
    """Abstract restricted list interface, representing a set of assets that an
    algorithm is restricted from trading.
    """

    @abc.abstractmethod
    def is_restricted(self, assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset], dt: datetime.datetime):
        """Is the asset restricted (RestrictionStates.FROZEN) on the given dt?

        Parameters
        ----------
        asset : Asset of iterable of Assets
            The asset(s) for which we are querying a restriction
        dt : pd.Timestamp
            The timestamp of the restriction query

        Returns
        -------
        is_restricted : bool or pd.Series[bool] indexed by asset
            Is the asset or assets restricted on this dt?

        """
        raise NotImplementedError("is_restricted")

    def __or__(self, other_restriction: 'Restrictions'):
        """Base implementation for combining two restrictions."""
        # If the right side is a _UnionRestrictions, defers to the
        # _UnionRestrictions implementation of `|`, which intelligently
        # flattens restricted lists
        from ziplime.finance.asset_restrictions.union_restrictions import _UnionRestrictions

        if isinstance(other_restriction, _UnionRestrictions):
            return other_restriction | self
        return _UnionRestrictions([self, other_restriction])
