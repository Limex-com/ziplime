"""Commission for params."""

from typing import Self
from typing import Literal
from ziplime.finance.commission.no_commission import NoCommission
from ziplime.finance.commission.per_contract import PerContract
from ziplime.finance.commission.per_dolar import PerDollar
from ziplime.finance.commission.per_share import PerShare
from ziplime.finance.commission.per_trade import PerTrade
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.sweep.params.strict import Strict
from ziplime.sweep.params.sweep_error import SweepError


class Commission(Strict):
    type: Literal["none", "per_share", "per_trade", "per_dollar", "per_contract"] = "none"
    cost: float = 0.0
    min_trade_cost: float = 0.0
    #: per_contract only: a flat exchange fee per order
    exchange_fee: float = 0.0

    def model(self):
        return {
            "none": lambda: NoCommission(),
            "per_share": lambda: PerShare(cost=self.cost, min_trade_cost=self.min_trade_cost),
            "per_trade": lambda: PerTrade(cost=self.cost),
            "per_dollar": lambda: PerDollar(cost=self.cost),
            "per_contract": lambda: PerContract(cost=self.cost, exchange_fee=self.exchange_fee,
                                                min_trade_cost=self.min_trade_cost),
        }[self.type]()

    @classmethod
    def of(cls, model: CommissionModel) -> Self:
        """The sweep's description of a ziplime commission model (set_commission in initialize)."""
        if model is None or isinstance(model, NoCommission):
            return cls()
        if isinstance(model, PerShare):
            return cls(type="per_share", cost=model.cost_per_share, min_trade_cost=model.min_trade_cost)
        if isinstance(model, PerDollar):
            return cls(type="per_dollar", cost=model.cost_per_dollar)
        if isinstance(model, PerTrade):
            return cls(type="per_trade", cost=model.cost)
        raise SweepError(f"Commission model {type(model).__name__} is not supported in a sweep.")
