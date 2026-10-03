from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.commission.commission_model import CommissionModel


class PerTrade(CommissionModel):
    """
    Calculates a commission for a transaction based on a per trade cost.

    For orders that require multiple fills, the full commission is charged to
    the first fill.

    Parameters
    ----------
    cost : float, optional
        The flat amount of commissions paid per equity trade.
    """

    def __init__(self, cost, ):
        """
        Cost parameter is the cost of a trade, regardless of share count.
        $5.00 per trade is fairly typical of discount exchanges.
        """
        # Cost needs to be floating point so that calculation using division
        # logic does not floor to an integer.
        self.cost = float(cost)

    def __repr__(self):
        return "{class_name}(cost_per_trade={cost})".format(
            class_name=self.__class__.__name__,
            cost=self.cost,
        )

    def calculate(self, order, transaction):
        """
        If the order hasn't had a commission paid yet, pay the fixed
        commission.
        """
        if order.commission == 0:
            # if the order hasn't had a commission attributed to it yet,
            # that's what we need to pay.
            return self.cost
        else:
            # order has already had commission attributed, so no more
            # commission.
            return 0.0

    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int, transaction_amount: float) -> float:
        # `raise self.cost` until now, which is a TypeError rather than a commission: a float is
        # not an exception. Nothing reached it while the only caller was the event-driven blotter,
        # which goes through `calculate`; the vector kernel prices every fill through this one.
        return self.cost
