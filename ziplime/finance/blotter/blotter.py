from abc import ABC, abstractmethod

from ziplime.assets.entities.asset import Asset
from ziplime.finance.cancel_policy import NeverCancel

from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.transaction import Transaction
from ziplime.domain.bar_data import BarData


class Blotter(ABC):
    def __init__(self, cancel_policy=None):
        self.cancel_policy = cancel_policy if cancel_policy else NeverCancel()

    @abstractmethod
    def save_order(self, order: Order) -> None: ...

    @abstractmethod
    def order_rejected(self, order: Order) -> None: ...

    @abstractmethod
    def order_held(self, order: Order) -> None: ...

    @abstractmethod
    def order_cancelled(self, order: Order) -> None: ...

    @abstractmethod
    def get_order_by_id(self, order_id: str, exchange_name: str) -> Order | None: ...

    @abstractmethod
    def get_open_orders_by_asset(self, asset: Asset, exchange_name: str) -> dict[str, Order] | None: ...

    @abstractmethod
    def get_open_orders(self,exchange_name: str) -> dict[Asset, dict[str, Order]]: ...

    @abstractmethod
    def get_all_assets_in_open_orders(self) -> list[Asset]: ...


    @abstractmethod
    def cancel_all_orders_for_asset(self, asset: Asset, exchange_name:str, relay_status: bool = True) -> None: ...

    def batch_order(self, order_arg_lists):
        """Place a batch of orders.

        Parameters
        ----------
        order_arg_lists : iterable[tuple]
            Tuples of args that `order` expects.

        Returns
        -------
        order_ids : list[str or None]
            The unique identifier (or None) for each of the orders placed
            (or not placed).

        Notes
        -----
        This is required for `Blotter` subclasses to be able to place a batch
        of orders, instead of being passed the order requests one at a time.
        """

        return [self.order(*order_args) for order_args in order_arg_lists]

    @abstractmethod
    def execute_cancel_policy(self, event):
        raise NotImplementedError("execute_cancel_policy")

    @abstractmethod
    def process_splits(self, splits: list[tuple[Asset, float]]) -> None:
        """
        Processes a list of splits by modifying any open orders as needed.

        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (asset, ratio).

        Returns
        -------
        None
        """

        raise NotImplementedError("process_splits")

    @abstractmethod
    def prune_orders(self, closed_orders) -> None:
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """

        raise NotImplementedError("prune_orders")
