"""Commission and slippage costs shared by sweep execution and its API."""

import dataclasses
from types import ModuleType

import numpy as np

from ziplime.sweep.backends import Backend
from ziplime.sweep.params import SweepError


@dataclasses.dataclass
class Costs:
    """Commission and slippage per instrument, as the engine's models compute them."""

    #: Per-unit models (PerShare, PerContract): cost per unit, minimum per order, exchange fee
    per_unit: np.ndarray
    min_trade: np.ndarray
    initial: np.ndarray
    unit_model: np.ndarray
    per_dollar: np.ndarray
    per_trade: np.ndarray
    #: FixedBasisPointsSlippage: share of price and of volume; FixedSlippage: spread
    percentage: np.ndarray
    spread: np.ndarray
    volume_limit: np.ndarray
    #: 0 none, 1 fixed basis points, 2 fixed spread
    slippage_kind: np.ndarray

    def on(self, backend: Backend) -> "Costs":
        return dataclasses.replace(self, **{
            field.name: backend.array(getattr(self, field.name)[None, :])
            for field in dataclasses.fields(self)})

    @property
    def order_state(self) -> bool:
        """Whether a fill's commission depends on earlier fills of its order."""
        return bool(np.any(np.asarray(self.min_trade) > 0) or np.any(np.asarray(self.initial) > 0)
                    or np.any(np.asarray(self.per_trade) > 0))

    host = None

    @property
    def capped_limit(self):
        """The volume share where a model caps by volume, 0 elsewhere (no inf × 0)."""
        return self.volume_limit * (self.slippage_kind == 1)

    def require_percent_sizing(self) -> None:
        kind = (self.host or self).slippage_kind
        if np.any(np.asarray(kind) == 2):
            raise SweepError("order_percent / order_target_percent with fixed_spread slippage fail "
                             "in the engine (FixedSlippage has no order_target_percentage_maximum_"
                             "quantity); use fixed_bps or none.")

    def projected(self, xp: 'ModuleType', quantity, value):
        """commission.calculate_for_asset: what sizing expects to pay."""
        unit = xp.maximum(self.min_trade, abs(quantity * self.per_unit) + self.initial)
        return xp.where(self.unit_model, unit, 0.0) + value * self.per_dollar + self.per_trade

    def maximum_quantity(self, xp: 'ModuleType', cash, now):
        """slippage.order_target_percentage_maximum_quantity: shares the cash buys at the close,
        priced as a buy, capped by what the bar's volume still allows."""
        close = now["spot_close"]
        price = close * (1 + self.percentage)
        capacity = xp.maximum(0.0, xp.floor(self.capped_limit * now["volume"]) - now["filled"])
        bps = xp.minimum(abs(cash / price), capacity)
        return xp.where(self.slippage_kind == 1, bps, abs(cash / close))
