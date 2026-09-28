from ziplime.finance.commission.per_contract import PerContract
from ziplime.utils.dummy import DummyMapping


class PerFutureTrade(PerContract):
    """
    Calculates a commission for a transaction based on a per trade cost.

    Parameters
    ----------
    cost : float or dict
        The flat amount of commissions paid per trade, regardless of the number
        of contracts being traded. If given a float, the commission for all
        futures contracts is the same. If given a dictionary, it must map root
        symbols to the commission cost for trading contracts of that symbol.
    """

    def __init__(self, cost,):  # =DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE):
        # The per-trade cost can be represented as the exchange fee in a
        # per-contract model because the exchange fee is just a one time cost
        # incurred on the first fill.
        super(PerFutureTrade, self).__init__(
            cost=0,
            exchange_fee=cost,
            min_trade_cost=0,
        )
        self._cost_per_trade = self._exchange_fee

    def __repr__(self):
        if isinstance(self._cost_per_trade, DummyMapping):
            # Cost per trade is a constant, so extract it.
            cost_per_trade = self._cost_per_trade["dummy key"]
        else:
            cost_per_trade = "<varies>"
        return "{class_name}(cost_per_trade={cost_per_trade})".format(
            class_name=self.__class__.__name__,
            cost_per_trade=cost_per_trade,
        )
