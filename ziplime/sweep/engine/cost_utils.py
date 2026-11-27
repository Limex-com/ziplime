"""Cost utils for engine."""

import numpy as np
from ziplime.sweep.costs import Costs
from ziplime.sweep.market import Market
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.sweep.params import SweepParams


def resolve_costs(params: SweepParams, market: Market) -> Costs:
    columns = len(market.listings)

    def per(field: str, equity, future):
        return np.where(market.is_future, future, equity).astype(float)

    def commission_parts(model: CommissionModel):
        kind = model.type
        return {
            "per_unit": model.cost if kind in ("per_share", "per_contract") else 0.0,
            "min_trade": model.min_trade_cost if kind in ("per_share", "per_contract") else 0.0,
            "initial": model.exchange_fee if kind == "per_contract" else 0.0,
            "unit_model": 1.0 if kind in ("per_share", "per_contract") else 0.0,
            "per_dollar": model.cost if kind == "per_dollar" else 0.0,
            "per_trade": model.cost if kind == "per_trade" else 0.0,
        }

    def slippage_parts(model: SlippageModel):
        return {
            "percentage": model.basis_points / 10_000 if model.type == "fixed_bps" else 0.0,
            "spread": model.spread if model.type == "fixed_spread" else 0.0,
            "volume_limit": model.volume_limit if model.type == "fixed_bps" else 0.0,
            "slippage_kind": {"none": 0, "fixed_bps": 1, "fixed_spread": 2}[model.type],
        }

    equity = {**commission_parts(params.equity_commission), **slippage_parts(params.equity_slippage)}
    future = {**commission_parts(params.future_commission), **slippage_parts(params.future_slippage)}
    return Costs(**{name: per(name, np.full(columns, equity[name]), np.full(columns, future[name]))
                    for name in equity})
