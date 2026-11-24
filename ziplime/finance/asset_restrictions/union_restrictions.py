"""Union restrictions for asset_restrictions."""

from functools import reduce
import operator
from collections.abc import Iterable
import datetime
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.restriction_utils import _is_one_asset
from ziplime.finance.asset_restrictions.restrictions import Restrictions


class _UnionRestrictions(Restrictions):
    """A union of a number of sub restrictions.

    Parameters
    ----------
    sub_restrictions : iterable of Restrictions (but not _UnionRestrictions)
        The Restrictions to be added together

    Notes
    -----
    - Consumers should not construct instances of this class directly, but
      instead use the `|` operator to combine restrictions
    """

    def __new__(cls, sub_restrictions: Iterable[Restrictions]):
        # Filter out NoRestrictions and deal with resulting cases involving
        # one or zero sub_restrictions
        from ziplime.finance.asset_restrictions.no_restrictions import NoRestrictions

        sub_restrictions = [
            r for r in sub_restrictions if not isinstance(r, NoRestrictions)
        ]
        if len(sub_restrictions) == 0:
            return NoRestrictions()
        elif len(sub_restrictions) == 1:
            return sub_restrictions[0]

        new_instance = super(_UnionRestrictions, cls).__new__(cls)
        new_instance.sub_restrictions = sub_restrictions
        return new_instance

    def __or__(self, other_restriction: Restrictions) -> '_UnionRestrictions':
        """Overrides the base implementation for combining two restrictions, of
        which the left side is a _UnionRestrictions.
        """
        # Flatten the underlying sub restrictions of _UnionRestrictions
        if isinstance(other_restriction, _UnionRestrictions):
            new_sub_restrictions = (
                self.sub_restrictions + other_restriction.sub_restrictions
            )
        else:
            new_sub_restrictions = self.sub_restrictions + [other_restriction]

        return _UnionRestrictions(new_sub_restrictions)

    def is_restricted(self, assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset], dt: datetime.datetime):
        if _is_one_asset(assets):
            return any(r.is_restricted(assets, dt) for r in self.sub_restrictions)

        return reduce(
            operator.or_,
            (r.is_restricted(assets, dt) for r in self.sub_restrictions),
        )
