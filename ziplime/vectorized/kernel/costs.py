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


from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.fixed_slippage import FixedSlippage
from ziplime.finance.slippage.no_slippage import NoSlippage
from ziplime.finance.slippage.slippage_model import SlippageModel

from ziplime.vectorized.kernel.models import VectorKernelError
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.vectorized.kernel.resolved_slippage import ResolvedSlippage
from ziplime.vectorized.kernel.resolved_commission import ResolvedCommission


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


def charge_through_model(model: CommissionModel, asset: ExchangeAsset, quantity: float,
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
