from collections.abc import Iterable

from ziplime.pipeline.terms.filters.static_sids import StaticSids
from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.exchange_asset import ExchangeAsset


class StaticAssets(StaticSids):
    """
    A Filter that computes True for a specific set of predetermined assets.

    ``StaticAssets`` is mostly useful for debugging or for interactively
    computing pipeline terms for a fixed set of assets that are known ahead of
    time.

    Parameters
    ----------
    assets : iterable[Asset]
        An iterable of assets for which to filter.
    """

    def __new__(cls, assets: 'Iterable[ExchangeAsset | ContinuousFuture]'):
        sids = frozenset(asset.sid for asset in assets)
        return super(StaticAssets, cls).__new__(cls, sids)
