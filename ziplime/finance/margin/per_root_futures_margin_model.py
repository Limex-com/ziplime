"""Per root futures margin model for margin."""

import dataclasses
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.margin.futures_margin_model import FuturesMarginModel


@dataclasses.dataclass
class PerRootFuturesMarginModel(FuturesMarginModel):
    """Margin quoted per contract per root, which is how exchanges publish it.

    Amounts are already in the currency the exchange collects, so this model needs no FX rate and
    is the right choice for a book that spans venues.

    Args:
        initial_by_root: ``root symbol -> amount per contract, in that root's margin currency``.
        default_per_contract: Used for a root that is not listed.
        maintenance_ratio: Maintenance margin as a share of initial margin.
    """

    initial_by_root: dict[str, float]
    default_per_contract: float = 0.0
    maintenance_ratio: float = 1.0
    models_margin = True

    def _per_contract(self, asset: ExchangeAsset) -> float:
        root_symbol = getattr(asset.asset, "root_symbol", None)
        return self.initial_by_root.get(root_symbol, self.default_per_contract)

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return abs(amount) * self._per_contract(asset)

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return self.initial_margin(asset, amount, price) * self.maintenance_ratio
