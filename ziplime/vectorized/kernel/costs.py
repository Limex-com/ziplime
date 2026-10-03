"""ziplime's own cost models, applied without the machinery they normally sit in.

The spec's §15 asks for ziplime cost semantics rather than a reimplementation of vectorbt's, and
that is not quite free: a commission model already offers an order-free entry point
(`calculate_for_asset`), but a slippage model does not. `SlippageModel.process_order` is `async`,
takes an `Order` and an `Exchange`, and asks the exchange for a spot value -- none of which exists
on a vector path, where there is no blotter, no order object and no exchange to query.

So slippage is read here as *parameters plus the arithmetic those parameters describe*, dispatched
on the model the caller passed. That is a small duplication of a formula, and it is deliberate:
the alternative is either building a fake `Order` and a fake `Exchange` per fill -- object churn
on the hot path, for an answer that is one multiplication -- or letting the vector path invent its
own slippage, which is exactly what the spec forbids.

The duplication is held honest by `tests/test_vector_kernel.py::SlippageParityTests`, which runs
each model both ways and requires the same price. A model this file has no formula for is refused
by name rather than silently filled at the untouched price: an unpaid cost does not surface as an
error, it surfaces as a better return.
"""
from __future__ import annotations

import dataclasses

from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.finance.slippage.slippage_model import SlippageModel

from .models import VectorKernelError


@dataclasses.dataclass(frozen=True)
class ResolvedSlippage:
    """A slippage model reduced to what the kernel needs: a price rule and a volume cap."""

    #: Fraction of the price added when buying, subtracted when selling. 0.0005 = 5bps.
    percentage: float = 0.0
    #: Absolute spread; half of it is added when buying, subtracted when selling.
    spread: float = 0.0
    #: Largest share of a bar's volume one order may take, or None for no cap.
    volume_limit: float | None = None

    def fill_price(self, price: float, direction: int) -> float:
        """The price an order of this direction fills at. `direction` is +1 buy, -1 sell.

        Mirrors `FixedBasisPointsSlippage.process_order` (`price + price * percentage * direction`)
        and `FixedSlippage.process_order` (`price + spread / 2 * direction`). Both, so a model
        carrying each is expressed without a second code path.
        """
        return price + price * self.percentage * direction + self.spread / 2.0 * direction

    def cap(self, wanted: float, volume: float | None) -> float:
        """How much of `wanted` the bar can absorb. Magnitudes, not signed amounts.

        A cap never turns an order around: the event-driven model clamps remaining capacity at
        zero for exactly this reason, and the same clamp is here. `None` volume means the caller
        gave no volume frame, which is "no opinion" rather than "a bar that trades nothing" --
        capping to zero there would silently stop every strategy that did not pass volumes.
        """
        if self.volume_limit is None or volume is None:
            return wanted
        return min(wanted, max(0.0, float(volume) * self.volume_limit))


def resolve_slippage(model: SlippageModel | None) -> ResolvedSlippage:
    """Reduce a ziplime slippage model to the kernel's terms, or refuse it by name."""
    if model is None or isinstance(model, NoSlippage):
        return ResolvedSlippage()
    if isinstance(model, FixedBasisPointsSlippage):
        return ResolvedSlippage(percentage=float(model.percentage),
                                volume_limit=float(model.volume_limit))
    if isinstance(model, FixedSlippage):
        return ResolvedSlippage(spread=float(model.spread))
    raise VectorKernelError(
        f"{type(model).__name__} cannot be applied on the vector path: it prices a fill from an "
        f"order and an exchange, and neither exists here. The vector kernel understands "
        f"NoSlippage, FixedSlippage and FixedBasisPointsSlippage.\n"
        f"For a strategy that needs a model like this one, keep it event-driven and vectorise its "
        f"signals instead (ziplime.vectorized.signals) -- the blotter fills every order there and "
        f"every slippage model applies."
    )


@dataclasses.dataclass(frozen=True)
class ResolvedCommission:
    """A commission model reduced to four numbers the compiled loop can use.

    The model itself offers `calculate_for_asset`, which is already order-free -- so until the
    inner loop was compiled there was nothing to reduce and this did not exist. Numba cannot call
    into an arbitrary Python object, and a commission is charged on the path that decides whether
    the *next* order fits, so it cannot be computed afterwards either.

    Every commission model the vector path supports fits one expression:

        max(min_trade, per_unit x |quantity| + per_dollar x |value| + fixed)

    NoCommission zeroes all four; PerShare sets `per_unit` and `min_trade`; PerTrade sets `fixed`;
    PerDollar sets `per_dollar`. `CommissionParityTests` runs each model both ways and requires
    the same number, which is what keeps this from drifting away from the models it stands for.
    """

    per_unit: float = 0.0
    per_dollar: float = 0.0
    fixed: float = 0.0
    min_trade: float = 0.0

    def charge(self, quantity: float, value: float) -> float:
        """One fill's commission. Magnitudes in, non-negative out."""
        return max(self.min_trade,
                   self.per_unit * abs(quantity) + self.per_dollar * abs(value) + self.fixed)


def resolve_commission(model: CommissionModel | None) -> ResolvedCommission:
    """Reduce a ziplime commission model to the kernel's terms, or refuse it by name."""
    if model is None or isinstance(model, NoCommission):
        return ResolvedCommission()
    if isinstance(model, PerShare):
        return ResolvedCommission(per_unit=float(model.cost_per_share),
                                  min_trade=float(model.min_trade_cost or 0.0))
    if isinstance(model, PerTrade):
        return ResolvedCommission(fixed=float(model.cost))
    if isinstance(model, PerDollar):
        return ResolvedCommission(per_dollar=float(model.cost_per_dollar))
    raise VectorKernelError(
        f"{type(model).__name__} cannot be applied on the vector path. The compiled kernel "
        f"charges commission inside the loop that decides whether the next order fits, so a model "
        f"has to reduce to numbers rather than to a call. Understood here: NoCommission, PerShare, "
        f"PerTrade, PerDollar.\n"
        f"Per-contract, per-option and bond models belong to instruments this path already "
        f"refuses; for anything else, keep the strategy event-driven and vectorise its signals "
        f"instead (ziplime.vectorized.signals), where every commission model applies."
    )


def charge_through_model(model: CommissionModel, asset, quantity: float,
                         transaction_amount: float) -> float:
    """One fill's commission, straight through the model's own code.

    Not used by the kernel -- `ResolvedCommission` is -- and that is exactly why it is here: the
    parity test needs the model's own arithmetic to compare the reduction against.
    """
    charged = model.calculate_for_asset(
        asset=asset, quantity=abs(quantity), transaction_amount=abs(transaction_amount))
    try:
        return float(charged)
    except (TypeError, ValueError) as error:
        raise VectorKernelError(
            f"{type(model).__name__}.calculate_for_asset returned {charged!r}, which is not a "
            f"number.") from error
