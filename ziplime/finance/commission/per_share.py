from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.commission import calculate_per_unit_commission
from ziplime.finance.commission.equity_commission_model import EquityCommissionModel


class PerShare(EquityCommissionModel):
    """
    Calculates a commission for a transaction based on a per share cost with
    an optional minimum cost per trade.

    Parameters
    ----------
    cost : float, optional
        The amount of commissions paid per share traded. Default is one tenth
        of a cent per share.
    min_trade_cost : float, optional
        The minimum amount of commissions paid per trade. Default is no
        minimum.

    Notes
    -----
    This is ziplime's default commission model for equities.
    """

    def __init__(
        self,
        cost,
        min_trade_cost,
    ):
        self.cost_per_share = cost
        self.min_trade_cost = min_trade_cost or 0

    def __repr__(self):
        return (
            "{class_name}(cost_per_share={cost_per_share}, "
            "min_trade_cost={min_trade_cost})".format(
                class_name=self.__class__.__name__,
                cost_per_share=self.cost_per_share,
                min_trade_cost=self.min_trade_cost,
            )
        )

    def calculate(self, order, transaction):
        return calculate_per_unit_commission(
            order=order,
            transaction=transaction,
            cost_per_unit=self.cost_per_share,
            initial_commission=0,
            min_trade_cost=self.min_trade_cost,
        )

    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int, transaction_amount: float) -> float:
        return max(self.min_trade_cost, abs(quantity * self.cost_per_share))
