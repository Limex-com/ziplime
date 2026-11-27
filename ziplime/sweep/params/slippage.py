"""Slippage for params."""

from typing import Self
from typing import Literal
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.sweep.params.strict import Strict
from ziplime.sweep.params.sweep_error import SweepError


class Slippage(Strict):
    type: Literal["none", "fixed_bps", "fixed_spread"] = "none"
    basis_points: float = 0.0
    spread: float = 0.0
    #: Share of a bar's volume an order may take (``fixed_bps`` only).
    volume_limit: float = 0.1

    def model(self):
        if self.type == "fixed_bps":
            return FixedBasisPointsSlippage(basis_points=self.basis_points,
                                            volume_limit=self.volume_limit)
        if self.type == "fixed_spread":
            return FixedSlippage(spread=self.spread)
        return NoSlippage()

    @classmethod
    def of(cls, model: SlippageModel) -> Self:
        """The sweep's description of a ziplime slippage model (set_slippage in initialize)."""
        if model is None or isinstance(model, NoSlippage):
            return cls()
        if isinstance(model, FixedBasisPointsSlippage):
            return cls(type="fixed_bps", basis_points=model.percentage * 10_000,
                       volume_limit=model.volume_limit)
        if isinstance(model, FixedSlippage):
            return cls(type="fixed_spread", spread=model.spread)
        raise SweepError(f"Slippage model {type(model).__name__} is not supported in a sweep.")
