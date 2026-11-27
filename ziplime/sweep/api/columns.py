"""Columns for api."""

import numpy as np
from ziplime.sweep.all_assets import ALL
from ziplime.sweep.vectorized_containers import VectorUniverse
from ziplime.sweep.params import SweepError
from ziplime.sweep.backends import Backend


class Columns:
    """Which column an instrument is."""

    def __init__(self, universe: dict, backend: Backend) -> None:
        self.names = list(universe)
        self.by_name = {name: k for k, name in enumerate(self.names)}
        self.by_asset = {listing: k for k, listing in enumerate(universe.values())}
        self.by_sid = {getattr(listing, "sid", None): k for k, listing in enumerate(universe.values())}
        self.backend = backend
        # Comparisons with the loop variable ("name == 'JNJ'") read the universe from here
        type(ALL)._columns = (self.names, list(universe.values()),
                              lambda values: backend.xp.array(values))

    def index(self, asset) -> int:
        if asset in self.by_asset:
            return self.by_asset[asset]
        if isinstance(asset, str) and asset in self.by_name:
            return self.by_name[asset]
        if isinstance(asset, int) and asset in self.by_sid:
            return self.by_sid[asset]
        raise SweepError(f"{asset!r} is not in context.universe: the sweep simulates the "
                         f"universe's instruments only.")

    def select(self, values, asset):
        if asset is ALL:
            return values
        column = self.index(asset)
        return values[:, column:column + 1]

    def only(self, asset):
        """(1 × instruments): True at this instrument."""
        if asset is ALL:
            return True
        mask = np.zeros((1, len(self.names)), dtype=bool)
        mask[0, self.index(asset)] = True
        return self.backend.xp.array(mask)

    def assets(self, assets) -> list:
        if assets is ALL or isinstance(assets, VectorUniverse):
            return [ALL]
        if isinstance(assets, dict):
            assets = list(assets.values())
        if not isinstance(assets, (list, tuple, set, frozenset)):
            return [assets]
        return list(assets)
