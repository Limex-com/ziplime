"""Asset restriction input normalization."""

from collections.abc import Iterable
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset


def _is_one_asset(assets: Asset | ExchangeAsset | Iterable[Asset | ExchangeAsset]) -> bool:
    """Whether ``assets`` is a single instrument rather than an iterable of them.

    Orders in this fork carry an :class:`ExchangeAsset` -- a listing, wrapping an
    :class:`Asset` rather than subclassing it -- so the ``_is_one_asset(assets)`` test these
    methods used was false for every order. Execution fell through to the vectorised branch, where
    ``pd.Index`` of a single listing raises ``TypeError``, which is what `set_asset_restrictions`
    did instead of restricting anything.
    """
    return isinstance(assets, (Asset, ExchangeAsset))
