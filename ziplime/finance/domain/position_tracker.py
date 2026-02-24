import uuid

import datetime
from collections import OrderedDict
from functools import partial
from math import isnan, copysign

import numpy as np
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.dividend import Dividend
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.position import Position
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.finance_ext import (
    PositionStats,
    calculate_position_tracker_stats
)


class PositionTracker:
    """The current state of the positions held.

    Parameters
    ----------
    data_frequency : {'daily', 'minute'}
        The data frequency of the simulation.
    """

    def __init__(self, exchanges: dict[str, Exchange], data_frequency: datetime.timedelta):
        self.positions = OrderedDict()

        self._unpaid_dividends = {}
        self._unpaid_stock_dividends = {}
        self._positions_store = {}

        self.data_frequency = data_frequency
        # self.data_bundle = data_bundle
        # cache the stats until something alters our positions
        self._dirty_stats = True
        self.exchanges = exchanges
        self._stats = PositionStats.new()
        self._logger = structlog.get_logger(__name__)

    def update_position(
            self,
            asset: Asset,
            amount: float | None = None,
            last_sale_price: float | None = None,
            last_sale_date=None,
            cost_basis=None,
    ):
        self._dirty_stats = True

        if asset not in self.positions:
            position = Position(asset,
                                amount=0,
                                cost_basis=float(0.0),
                                last_sale_price=float(0.0),
                                last_sale_date=None)
            self.positions[asset] = position
        else:
            position = self.positions[asset]

        if amount is not None:
            position.amount = amount
        if last_sale_price is not None:
            position.last_sale_price = last_sale_price
        if last_sale_date is not None:
            position.last_sale_date = last_sale_date
        if cost_basis is not None:
            position.cost_basis = cost_basis

    def execute_transaction(self, txn):
        self._dirty_stats = True

        asset = txn.asset

        if asset not in self.positions:
            position = Position(asset=asset,
                                amount=0,
                                cost_basis=0.0,
                                last_sale_price=0.0,
                                last_sale_date=None
                                )
            self.positions[asset] = position
        else:
            position = self.positions[asset]

        self._update_position(position=position, txn=txn)

        if position.amount == 0:
            del self.positions[asset]

            try:
                # if this position exists in our user-facing dictionary,
                # remove it as well.
                del self._positions_store[asset]
            except KeyError:
                pass

    def _update_position(self, position: Position, txn: Transaction):
        if position.asset != txn.asset:
            raise Exception("updating position with txn for a " "different asset")

        total_shares = position.amount + txn.amount

        if total_shares == 0:
            position.cost_basis = 0.0
        else:
            prev_direction = copysign(1, position.amount)
            txn_direction = copysign(1, txn.amount)

            if prev_direction != txn_direction:
                # we're covering a short or closing a position
                if abs(txn.amount) > abs(position.amount):
                    # we've closed the position and gone short
                    # or covered the short position and gone long
                    position.cost_basis = txn.price
            else:
                prev_cost = position.cost_basis * position.amount
                txn_cost = txn.amount * txn.price
                total_cost = prev_cost + txn_cost
                position.cost_basis = total_cost / total_shares

            # Update the last sale price if txn is
            # best data we have so far
            if position.last_sale_date is None or txn.dt > position.last_sale_date:
                position.last_sale_price = txn.price
                position.last_sale_date = txn.dt

        position.amount = total_shares

    def handle_commission(self, asset: Asset, cost: float) -> None:
        # Adjust the cost basis of the stock if we own it
        if asset in self.positions:
            self._dirty_stats = True
            self.adjust_commission_cost_basis(position=self.positions[asset], asset=asset, cost=cost)

    def adjust_commission_cost_basis(self, position: Position, asset: Asset, cost: float):
        """
        A note about cost-basis in ziplime: all positions are considered
        to share a cost basis, even if they were executed in different
        transactions with different commission costs, different prices, etc.

        Due to limitations about how ziplime handles positions, ziplime will
        currently spread an externally-delivered commission charge across
        all shares in a position.
        """

        if asset != position.asset:
            raise Exception("Updating a commission for a different asset?")
        if cost == 0.0:
            return

        # If we no longer hold this position, there is no cost basis to
        # adjust.
        if position.amount == 0:
            return

        # We treat cost basis as the share price where we have broken even.
        # For longs, commissions cause a relatively straight forward increase
        # in the cost basis.
        #
        # For shorts, you actually want to decrease the cost basis because you
        # break even and earn a profit when the share price decreases.
        #
        # Shorts are represented as having a negative `amount`.
        #
        # The multiplication and division by `amount` cancel out leaving the
        # cost_basis positive, while subtracting the commission.

        prev_cost = position.cost_basis * position.amount
        if isinstance(asset, FuturesContract):
            cost_to_use = cost / asset.price_multiplier
        else:
            cost_to_use = cost
        new_cost = prev_cost + cost_to_use
        position.cost_basis = new_cost / position.amount

    def handle_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.

        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (asset, ratio).

        Returns
        -------
        int: The leftover cash from fractional shares after modifying each
            position.
        """
        total_leftover_cash = 0

        for asset, ratio in splits:
            if asset in self.positions:
                self._dirty_stats = True

                # Make the position object handle the split. It returns the
                # leftover cash from a fractional share, if there is any.
                position = self.positions[asset]
                leftover_cash = self.handle_split(position=position, asset=asset, ratio=ratio)
                total_leftover_cash += leftover_cash

        return total_leftover_cash

    def earn_dividend(self, position: Position, dividend: Dividend) -> dict[str, float]:
        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        return {"amount": position.amount * dividend.amount}

    def earn_stock_dividend(self, position: Position, stock_dividend):
        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        return {
            "payment_asset": stock_dividend.payment_asset,
            "share_count": np.floor(position.amount * float(stock_dividend.ratio)),
        }

    def handle_split(self, position: Position, asset: Asset, ratio: float):
        """
        Update the position by the split ratio, and return the resulting
        fractional share that will be converted into cash.

        Returns the unused cash.
        """
        if position.asset != asset:
            raise Exception("updating split with the wrong asset!")

        # adjust the # of shares by the ratio
        # (if we had 100 shares, and the ratio is 3,
        #  we now have 33 shares)
        # (old_share_count / ratio = new_share_count)
        # (old_price * ratio = new_price)

        # e.g., 33.333
        raw_share_count = position.amount / float(ratio)

        # e.g., 33
        full_share_count = np.floor(raw_share_count)

        # e.g., 0.333
        fractional_share_count = raw_share_count - full_share_count

        # adjust the cost basis to the nearest cent, e.g., 60.0
        new_cost_basis = round(position.cost_basis * ratio, 2)

        position.cost_basis = new_cost_basis
        position.amount = full_share_count

        return_cash = round(float(fractional_share_count * new_cost_basis), 2)

        self._logger.info("after split: " + str(position))
        self._logger.info("returning cash: " + str(return_cash))

        # return the leftover cash, which will be converted into cash
        # (rounded to the nearest cent)
        return return_cash

    def earn_dividends(self, cash_dividends, stock_dividends):
        """Given a list of dividends whose ex_dates are all the next trading
        day, calculate and store the cash and/or stock payments to be paid on
        each dividend's pay date.

        Parameters
        ----------
        cash_dividends : iterable of (asset, amount, pay_date) namedtuples

        stock_dividends: iterable of (asset, payment_asset, ratio, pay_date)
            namedtuples.
        """
        for cash_dividend in cash_dividends:
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            div_owed = self.earn_dividend(position=self.positions[cash_dividend.asset], dividend=cash_dividend)
            try:
                self._unpaid_dividends[cash_dividend.pay_date].append(div_owed)
            except KeyError:
                self._unpaid_dividends[cash_dividend.pay_date] = [div_owed]

        for stock_dividend in stock_dividends:
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            div_owed = self.earn_stock_dividend(position=self.positions[stock_dividend.asset],
                                                stock_dividend=stock_dividend)
            try:
                self._unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                self._unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def pay_dividends(self, next_trading_day: datetime.datetime):
        """
        Returns a cash payment based on the dividends that should be paid out
        according to the accumulated bookkeeping of earned, unpaid, and stock
        dividends.
        """
        net_cash_payment = 0.0

        try:
            payments = self._unpaid_dividends[next_trading_day]
            # Mark these dividends as paid by dropping them from our unpaid
            del self._unpaid_dividends[next_trading_day]
        except KeyError:
            payments = []

        # representing the fact that we're required to reimburse the owner of
        # the stock for any dividends paid while borrowing.
        for payment in payments:
            net_cash_payment += payment["amount"]

        # Add stock for any stock dividends paid.  Again, the values here may
        # be negative in the case of short positions.
        try:
            stock_payments = self._unpaid_stock_dividends[next_trading_day]
        except KeyError:
            stock_payments = []

        for stock_payment in stock_payments:
            payment_asset = stock_payment["payment_asset"]
            share_count = stock_payment["share_count"]
            # note we create a Position for stock dividend if we don't
            # already own the asset
            if payment_asset in self.positions:
                position = self.positions[payment_asset]
            else:
                position = self.positions[payment_asset] = Position(
                    asset=payment_asset,
                    amount=0,
                    cost_basis=0.0,
                    last_sale_price=0.0,
                    last_sale_date=None

                )

            position.amount += share_count

        return net_cash_payment

    def maybe_create_close_position_transaction(self, asset: Asset, dt: datetime.datetime):
        if not self.positions.get(asset):
            return None

        amount = self.positions.get(asset).amount
        # TODO: check this
        price = self.data_bundle.get_spot_value(assets=asset, field="price", dt=dt,
                                                data_frequency=self._data_frequency)

        # Get the last traded price if price is no longer available
        if isnan(price):
            price = self.positions.get(asset).last_sale_price

        return Transaction(
            id=uuid.uuid4().hex,
            asset=asset,
            amount=-amount,
            dt=dt,
            price=price,
            order_id=None,
            exchange_name=None
        )

    def get_positions(self):
        for asset, pos in self.positions.items():
            # Adds the new position if we didn't have one before, or overwrite
            # one we have currently
            self._positions_store[asset] = pos

        return self._positions_store

    def get_position_list(self):
        return [
            pos for asset, pos in self.positions.items() if pos.amount != 0
        ]

    def sync_last_sale_prices(self, dt: datetime.datetime,
                              exchange_name:str,
                              handle_non_market_minutes: bool = False):
        self._dirty_stats = True
        exchange = self.exchanges[exchange_name]
        if handle_non_market_minutes:
            previous_minute = exchange.trading_calendar.previous_minute(minute=dt)
            get_price = partial(
                exchange.get_adjusted_value,
                field="close",
                dt=previous_minute,
                perspective_dt=dt,
                frequency=self.data_frequency
            )

        else:
            get_price = partial(
                exchange.get_scalar_asset_spot_value_sync,
                field="close",
                dt=dt,
                frequency=self.data_frequency
            )
        for outer_position in self.positions.values():
            inner_position = outer_position

            last_sale_price = get_price(inner_position.asset)["close"][0]

            # inline ~isnan because this gets called once per position per minute
            if last_sale_price is None:

                self._logger.warning(
                    f"Error updating last sale price for {inner_position.asset.asset_name} on {dt}. Price is None")
            else:  # last_sale_price == last_sale_price:
                inner_position.last_sale_price = last_sale_price
                inner_position.last_sale_date = dt

    @property
    def stats(self):
        """The current status of the positions.

        Returns
        -------
        stats : PositionStats
            The current stats position stats.

        Notes
        -----
        This is cached, repeated access will not recompute the stats until
        the stats may have changed.
        """
        if self._dirty_stats:
            calculate_position_tracker_stats(self.positions, self._stats)
            self._dirty_stats = False

        return self._stats
