from abc import abstractmethod

from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset

from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.shared import FinancialModelMeta

class CommissionModel(metaclass=FinancialModelMeta):
    """Abstract base class for commission models.

    Commission models are responsible for accepting order/transaction pairs and
    calculating how much commission should be charged to an algorithm's account
    on each transaction.

    To implement a new commission model, create a subclass of
    :class:`~ziplime.finance.commission.CommissionModel` and implement
    :meth:`calculate`.
    """

    # Asset types that are compatible with the given model.
    allowed_asset_types = (Equity, Bond, FuturesContract)

    @abstractmethod
    def calculate(self, order, transaction):
        """
        Calculate the amount of commission to charge on ``order`` as a result
        of ``transaction``.

        Parameters
        ----------
        order : ziplime.finance.order.Order
            The order being processed.

            The ``commission`` field of ``order`` is a float indicating the
            amount of commission already charged on this order.

        transaction : ziplime.finance.transaction.Transaction
            The transaction being processed. A single order may generate
            multiple transactions if there isn't enough volume in a given bar
            to fill the full amount requested in the order.

        Returns
        -------
        amount_charged : float
            The additional commission, in dollars, that we should attribute to
            this order.
        """
        raise NotImplementedError("calculate")

    @abstractmethod
    def calculate_for_asset(self, asset: ExchangeAsset, quantity: int, transaction_amount: float) -> float:
        raise NotImplementedError("calculate_for_asset")
