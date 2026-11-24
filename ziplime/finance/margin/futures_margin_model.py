"""Futures margin model for margin."""

from ziplime.assets.entities.exchange_asset import ExchangeAsset


class FuturesMarginModel:
    """Base class: how much capital a futures position ties up, and in which currency."""

    #: False for the placeholder model, so callers can tell "no margin" from "zero margin".
    models_margin = True

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        """Margin required to open ``amount`` contracts at ``price``, in ``asset``'s margin currency."""
        raise NotImplementedError("initial_margin")

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        """Margin that must remain posted while the position is held."""
        raise NotImplementedError("maintenance_margin")
