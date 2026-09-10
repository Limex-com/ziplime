from collections import defaultdict, OrderedDict
from copy import copy

import structlog

from .blotter import Blotter

from ziplime.domain.bar_data import BarData
from ziplime.finance.domain.order import Order
from ziplime.exchanges.exchange import Exchange
from ...assets.entities.asset import Asset
from ...assets.entities.exchange_asset import ExchangeAsset
from ...exchanges.repositories.exchange_repository import ExchangeRepository


class InMemoryBlotter(Blotter):
    def __init__(
            self,
            exchanges : list[Exchange],
            cancel_policy,
            new_orders: dict = None
    ):
        super().__init__(cancel_policy=cancel_policy)
        self._logger = structlog.get_logger(__name__)
        # these orders are aggregated by asset
        self.open_orders = {exchange.name: defaultdict(dict) for exchange in exchanges}
        # keep a dict of orders by their own id
        self.orders = {exchange.name: {} for exchange in exchanges}
        # holding orders that have come in since the last event.
        self.new_orders = new_orders or OrderedDict()

        self.max_shares = int(1e11)

    def save_order(self, order: Order):
        """Place an order.

        Parameters
        ----------
        asset : ziplime.assets.Asset
            The asset that this order is for.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : ziplime.finance.execution.ExecutionStyle
            The execution style for the order.
        order_id : str, optional
            The unique identifier for this order.

        Returns
        -------
        order_id : str or None
            The unique identifier for this order, or None if no order was
            placed.

        Notes
        -----
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(asset, amount)
        Limit order:     order(asset, amount, style=LimitOrder(limit_price))
        Stop order:      order(asset, amount, style=StopOrder(stop_price))
        StopLimit order: order(asset, amount, style=StopLimitOrder(limit_price,
        stop_price))
        """
        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount
        self.open_orders[order.exchange_name][order.asset][order.id] = order
        self.orders[order.exchange_name][order.id] = order
        return order.id

    def order_cancelled(self, order: Order) -> None:
        # Keyed by the listing, matching `save_order`. Reading by `order.asset.sid` looked up a key
        # that is never written, and because `open_orders` is a defaultdict the miss created an
        # empty entry instead of raising -- so a cancelled order was never removed and stayed
        # visible as open for the rest of the simulation.
        self._forget_order(order)

    def order_rejected(self, order: Order) -> None:
        self._forget_order(order)

    def _forget_order(self, order: Order) -> None:
        """Drop ``order`` from the open orders, and the listing too once it has none left."""
        exchange_orders = self.open_orders.get(order.exchange_name)
        if exchange_orders is None:
            return
        asset_orders = exchange_orders.get(order.asset)
        if asset_orders is None:
            return
        asset_orders.pop(order.id, None)
        if not asset_orders:
            exchange_orders.pop(order.asset, None)

    def get_order_by_id(self, order_id: str, exchange_name: str) -> Order | None:
        return self.orders.get(exchange_name, {}).get(order_id, None)

    def get_open_orders_by_asset(self, asset: ExchangeAsset, exchange_name: str) -> dict[str, Order] | None:
        return self.open_orders.get(exchange_name, {}).get(asset, None)

    def get_open_orders(self, exchange_name: str) -> dict[ExchangeAsset, dict[str, Order]]:
        return self.open_orders.get(exchange_name, {})

    def get_all_assets_in_open_orders(self) -> list[ExchangeAsset]:
        return [asset for asset_orders in self.open_orders.values() for asset in asset_orders]

    def cancel_all_orders_for_asset(self, asset: ExchangeAsset, exchange_name: str, relay_status: bool = True):
        """
        Cancel all open orders for a given asset.
        """
        self.open_orders.get(exchange_name, {}).pop(asset, None)

    # End of day cancel for daily frequency
    def execute_daily_cancel_policy(self, event):
        if self.cancel_policy.should_cancel(event):
            warn = self.cancel_policy.warn_on_cancel
            for asset in copy(self.open_orders):
                orders = self.open_orders[asset]
                if len(orders) > 1:
                    order = orders[0]
                    self.cancel(order.id, relay_status=True)
                    if warn:
                        if order.filled > 0:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} has been partially filled. "
                                f"{order.filled} shares were successfully "
                                f"purchased. {order.amount - order.filled} shares were not "
                                f"filled by the end of day and "
                                f"were canceled."
                            )
                        elif order.filled < 0:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} has been partially filled. "
                                f"{-1 * order.filled} shares were successfully "
                                f"sold. {-1 * (order.amount - order.filled)} shares were not "
                                f"filled by the end of day and "
                                f"were canceled."
                            )
                        else:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} failed to fill by the end of day "
                                f"and was canceled."
                            )

    def execute_cancel_policy(self, event):
        if self.cancel_policy.should_cancel(event):
            warn = self.cancel_policy.warn_on_cancel
            for exchange in self.open_orders:
                for asset in self.open_orders[exchange]:
                    self.cancel_all_orders_for_asset(asset=asset, exchange_name=exchange, relay_status=False)

    def order_held(self, order: Order) -> None:
        pass

    def process_splits(self, splits):
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

        for split in splits:
            for exchange, asset_positions in self.open_orders.items():
                for asset, orders in asset_positions.items():
                    for order_id, order in orders.items():
                        if order.asset.asset.id == split.asset.id:
                            order.handle_split(split.ratio)


            # if split.asset not in self.open_orders:
            #     continue
            #
            # orders_to_modify = self.open_orders[split.asset]
            # for order in orders_to_modify:
            #     order.handle_split(split.ratio)

    # def get_transactions(self, bar_data: BarData):
    #     """
    #     Creates a list of transactions based on the current open orders,
    #     slippage model, and commission model.
    #
    #     Parameters
    #     ----------
    #     bar_data: ziplime.protocol.BarData
    #
    #     Notes
    #     -----
    #     This method book-keeps the blotter's open_orders dictionary, so that
    #      it is accurate by the time we're done processing open orders.
    #
    #     Returns
    #     -------
    #     transactions_list: List
    #         transactions_list: list of transactions resulting from the current
    #         open orders.  If there were no open orders, an empty list is
    #         returned.
    #
    #     commissions_list: List
    #         commissions_list: list of commissions resulting from filling the
    #         open orders.  A commission is an object with "asset" and "cost"
    #         parameters.
    #
    #     closed_orders: List
    #         closed_orders: list of all the orders that have filled.
    #     """
    #
    #     closed_orders = []
    #     transactions = []
    #     commissions = []
    #
    #     if self.open_orders:
    #         for asset, asset_orders in self.open_orders.items():
    #             slippage = self.exchange.get_slippage_model(asset=asset)
    #
    #             for order, txn in slippage.simulate(data=bar_data, assets=frozenset({asset}),
    #                                                 orders_for_asset=asset_orders.values()):
    #                 commission = self.exchange.get_commission_model(asset=asset)
    #                 additional_commission = commission.calculate(order, txn)
    #
    #                 if additional_commission > 0:
    #                     commissions.append(
    #                         {
    #                             "asset": order.asset,
    #                             "order": order,
    #                             "cost": additional_commission,
    #                         }
    #                     )
    #
    #                 order.filled += txn.amount
    #                 order.commission += additional_commission
    #
    #                 order.dt = txn.dt
    #
    #                 transactions.append(txn)
    #
    #                 if not order.open:
    #                     closed_orders.append(order)
    #
    #     return transactions, commissions, closed_orders

    def prune_orders(self, closed_orders):
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """
        # remove all closed orders from our open_orders dict
        for order in closed_orders:
            asset = order.asset
            asset_orders = self.open_orders[order.exchange_name][asset]
            asset_orders.pop(order.id, None)

        # now clear out the assets from our open_orders dict that have
        # zero open orders
        for exchange_name, exchange_orders in self.open_orders.items():
            for asset in list(exchange_orders.keys()):
                if len(self.open_orders[exchange_name][asset]) == 0:
                    self.open_orders.get(exchange_name, {}).pop(asset, None)
