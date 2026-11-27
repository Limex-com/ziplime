"""Positions for portfolio."""

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.entities.exchange_asset import ExchangeAsset


class Positions(dict):
    """``(exchange, trading account, asset) -> Position``, also looked up by the asset alone.

    The map is keyed by book, but strategies written for zipline ask
    ``context.portfolio.positions.get(asset)`` or ``positions[asset].amount``. Those lookups used
    to miss every time -- ``get`` returned its default and ``in`` was False whatever was held -- so
    a strategy that closed what it held by checking its position never closed anything. Here an
    asset key finds the asset's position; held in several books, it is ambiguous and raises
    rather than picking one. Iteration and the tuple keys are unchanged.
    """

    def _by_asset(self, asset: ExchangeAsset | ContinuousFuture):
        sid = getattr(asset, "sid", None)
        found = [position for key, position in dict.items(self)
                 if key[-1] == asset or (sid is not None and getattr(key[-1], "sid", None) == sid)]
        if len(found) > 1:
            raise ValueError(f"{asset} is held in {len(found)} books; look it up by "
                             f"(exchange, trading account, asset).")
        return found[0] if found else None

    def __getitem__(self, key: tuple[str, str, ExchangeAsset] | ExchangeAsset | ContinuousFuture):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        position = self._by_asset(key)
        if position is None:
            raise KeyError(key)
        return position

    def get(self, key: tuple[str, str, ExchangeAsset] | ExchangeAsset | ContinuousFuture, default=None):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.get(self, key, default)
        position = self._by_asset(key)
        return default if position is None else position

    def __contains__(self, key: tuple[str, str, ExchangeAsset] | ExchangeAsset | ContinuousFuture):
        if isinstance(key, tuple) or dict.__contains__(self, key):
            return dict.__contains__(self, key)
        return self._by_asset(key) is not None
