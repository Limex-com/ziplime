import uuid

import datetime
from collections import OrderedDict
from functools import partial
from math import isnan, copysign

import numpy as np
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.dividend_payout import DividendPayout
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.bonds import BondBook
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.position import Position
from ziplime.finance.domain.transaction import Transaction
from ziplime.finance.finance_ext import (
    PositionStats,
    calculate_position_tracker_stats
)
import polars as pl


class PositionTracker:
    """The current state of the positions held.

    Parameters
    ----------
    data_frequency : {'daily', 'minute'}
        The data frequency of the simulation.
    """

    def __init__(self, data_frequency: datetime.timedelta, bond_book: BondBook | None = None):

        # (exchange_name, trading_account_id, asset)
        self.positions = OrderedDict()
        # (exchange_name, trading_account_id)
        self.positions_by_trading_account = {}
        # (asset,)
        self.positions_by_asset = {}
        # (exchange_name,)
        self.positions_by_exchange = {}

        self._unpaid_dividends = {}
        self._unpaid_stock_dividends = {}
        #: pay date -> money owed, from coupons and amortization instalments already earned.
        self._unpaid_bond_payments: dict[datetime.date, float] = {}
        #: Coupon and amortization schedules, shared with the ledger that owns this tracker.
        self.bond_book = bond_book if bond_book is not None else BondBook()
        # self._positions_store = {}

        self.data_frequency = data_frequency
        # self.data_bundle = data_bundle
        # cache the stats until something alters our positions
        self._dirty_stats = True
        self._stats = PositionStats.new()
        self._logger = structlog.get_logger(__name__)
        self.position_trades = {}

    def _register_position(
            self,
            position: Position,
            position_key: tuple,
            position_key_trading_account: tuple,
            position_key_asset: tuple,
            position_key_exchange: tuple,
    ) -> None:
        # Every index holds a reference to the very same Position object;
        # only the keys are duplicated.
        self.positions[position_key] = position
        self.positions_by_trading_account.setdefault(
            position_key_trading_account, set()
        ).add(position)
        self.positions_by_asset.setdefault(position_key_asset, set()).add(position)
        self.positions_by_exchange.setdefault(position_key_exchange, set()).add(position)

    def _unregister_position(self, position: Position) -> None:
        del self.positions[
            (position.exchange_name, position.trading_account_id, position.asset)
        ]
        for positions_by_key, key in (
                (self.positions_by_trading_account, (position.exchange_name, position.trading_account_id)),
                (self.positions_by_asset, (position.asset,)),
                (self.positions_by_exchange, (position.exchange_name,))
        ):
            positions = positions_by_key.get(key)
            if positions is None:
                continue
            positions.discard(position)
            if not positions:
                del positions_by_key[key]

    def update_position(
            self,
            asset: ExchangeAsset,
            exchange_name: str,
            trading_account_id: str,
            amount: int | None = None,
            last_sale_price: float | None = None,
            last_sale_date=None,
            cost_basis=None,
    ) -> Position:
        self._dirty_stats = True

        position_key = (exchange_name, trading_account_id, asset)
        position_key_trading_account = (exchange_name, trading_account_id)
        position_key_asset = (asset,)
        position_key_exchange = (exchange_name,)

        if position_key not in self.positions:
            position = Position(
                asset=asset,
                exchange_name=exchange_name,
                trading_account_id=trading_account_id,
                amount=0,
                cost_basis=float(0.0),
                last_sale_price=float(0.0),
                last_sale_date=None,
            )
            self._register_position(
                position=position,
                position_key=position_key,
                position_key_trading_account=position_key_trading_account,
                position_key_asset=position_key_asset,
                position_key_exchange=position_key_exchange,
            )
        else:
            position = self.positions[position_key]

        if amount is not None:
            position.amount = amount
        if last_sale_price is not None:
            position.last_sale_price = last_sale_price
        if last_sale_date is not None:
            position.last_sale_date = last_sale_date
        if cost_basis is not None:
            position.cost_basis = cost_basis
        return position

    def execute_transaction(self, txn):
        self._dirty_stats = True

        # asset = txn.asset
        # position_key = (asset, txn.exchange)
        #
        # if position_key not in self.positions:
        #     position = Position(
        #         asset=asset,
        #         exchange=exchange,
        #         trading_account_id=trading_account_id,
        #         amount=0,
        #         cost_basis=float(0.0),
        #         last_sale_price=float(0.0),
        #         last_sale_date=None,
        #     )
        #
        #     self.positions[position_key][trading_account_id] = position
        # else:
        #     position = self.positions[asset]
        position = self.update_position(
            asset=txn.asset,
            exchange_name=txn.exchange_name,
            trading_account_id=txn.trading_account_id,
            amount=None,
            last_sale_price=None,
            last_sale_date=None,
            cost_basis=None
        )

        self._update_position(position=position, txn=txn)
        if position.amount == 0:
            self._unregister_position(position)

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

    def handle_commission(self, asset: ExchangeAsset, cost: float) -> None:
        # Adjust the cost basis of the stock if we own it
        positions = self.positions_by_asset.get((asset,))
        if not positions:
            return
        self._dirty_stats = True
        for position in positions:
            self.adjust_commission_cost_basis(position=position, cost=cost)

    def adjust_commission_cost_basis(self, position: Position, cost: float):
        """
        A note about cost-basis in ziplime: all positions are considered
        to share a cost basis, even if they were executed in different
        transactions with different commission costs, different prices, etc.

        Due to limitations about how ziplime handles positions, ziplime will
        currently spread an externally-delivered commission charge across
        all shares in a position.
        """

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
        instrument = position.asset.asset
        if isinstance(instrument, FuturesContract):
            cost_to_use = cost / instrument.multiplier
        elif isinstance(instrument, Bond):
            # A bond's cost basis is carried in quote units (percent of face), so a commission in
            # money has to be converted before it can be folded in.
            per_point = self.bond_book.money_per_quote_unit(instrument, position.last_sale_date)
            cost_to_use = cost / per_point if per_point else cost
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

        for split in splits:
            for position in self.positions.values():
                if position.asset.asset.id == split.asset.id:
                    self._dirty_stats = True
                    leftover_cash = self.handle_split(position=position, asset=split.asset, ratio=split.ratio)
                    total_leftover_cash += leftover_cash

        # for split in splits:
        #     if split.asset in self.positions:
        #         self._dirty_stats = True
        #
        #         # Make the position object handle the split. It returns the
        #         # leftover cash from a fractional share, if there is any.
        #         position = self.positions[split.asset]
        #         leftover_cash = self.handle_split(position=position, asset=split.asset, ratio=split.ratio)
        #         total_leftover_cash += leftover_cash

        return total_leftover_cash

    def earn_dividend(self, position: Position, dividend: DividendPayout) -> dict[str, float]:
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
        if position.asset.asset != asset:
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
            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            divs_owed = [
                self.earn_dividend(position=position, dividend=cash_dividend)
                for position in self.positions.values()
                if position.asset.asset.id == cash_dividend.asset.id
            ]
            if not divs_owed:
                continue
            self._dirty_stats = True  # only mark dirty if we pay a dividend
            try:
                self._unpaid_dividends[cash_dividend.pay_date].extend(divs_owed)
            except KeyError:
                self._unpaid_dividends[cash_dividend.pay_date] = divs_owed

        for stock_dividend in stock_dividends:
            position = next(
                (
                    position
                    for position in self.positions.values()
                    if position.asset.asset.id == stock_dividend.asset.id
                ),
                None,
            )
            if position is None:
                continue
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            div_owed = self.earn_stock_dividend(position=position, stock_dividend=stock_dividend)
            try:
                self._unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                self._unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def earn_bond_payments(self, bond_events: list[BondEvent]) -> None:
        """Record what the coupons and amortizations in ``bond_events`` will pay us.

        Entitlement is settled on the record date and paid later, so this snapshots the position
        size now and the money moves in :meth:`pay_bond_payments`. Selling in between does not
        forfeit the payment, which is what a record date means; a short position owes it, and the
        negative amount carries that through.

        Holdings are summed across every exchange and account, so the same coupon is never earned
        twice and a position split over two accounts is paid in full.
        """
        for event in bond_events:
            held = sum(position.amount
                       for accounts in self.positions.values()
                       for positions in accounts.values()
                       for position in positions.values()
                       if isinstance(position.asset.asset, Bond)
                       and position.asset.asset.id == event.asset.id)
            if held == 0:
                continue
            self._dirty_stats = True
            owed = held * event.value
            self._unpaid_bond_payments[event.date] = (
                self._unpaid_bond_payments.get(event.date, 0.0) + owed)

    def pay_bond_payments(self, session: datetime.date) -> float:
        """Cash from every coupon and amortization instalment due on or before ``session``.

        On or *before*, not on: a payment dated to a weekend or an exchange holiday still has to
        reach the account, and it reaches it on the next session rather than never.

        Negative for a short position: a short bond seller owes the coupon to whoever lent them
        the paper, exactly as a short equity owes the dividend.
        """
        due = [date for date in self._unpaid_bond_payments if date <= session]
        return sum(self._unpaid_bond_payments.pop(date) for date in due)

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

            positions = self.positions_by_asset.get((payment_asset,))
            if not positions:
                self._logger.warning(
                    "Not paying stock dividend of {} shares of {}: no open "
                    "position for the payment asset".format(
                        share_count,
                        getattr(payment_asset, "asset_name", payment_asset)
                    )
                )
                continue
            for position in positions:
                position.amount += share_count

        return net_cash_payment

    def close_positions(self, asset: ExchangeAsset, dt: datetime.datetime) -> list[Transaction]:
        """Create a closing transaction for every open position in ``asset``.

        The position's last sale price is used as the close price, since the
        tracker has no direct access to market data.
        """
        positions = self.positions_by_asset.get((asset,))
        if not positions:
            return []
        return [
            Transaction(
                id=uuid.uuid4().hex,
                asset=asset,
                amount=-position.amount,
                dt=dt,
                price=position.last_sale_price,
                order_id=None,
                exchange_name=position.exchange_name,
                trading_account_id=position.trading_account_id,
            )
            for position in positions
        ]

    def get_positions(self):
        return self.positions
        # print(f"Get positions, len={len(self.positions)}")
        # for asset, pos in self.positions.items():
        #     # Adds the new position if we didn't have one before, or overwrite
        #     # one we have currently
        #     self._positions_store[asset] = pos
        #
        # return self._positions_store

    def get_position_list(self):
        return [
            position
            for position in self.positions.values()
            if position.amount != 0
        ]

    def sync_last_sale_prices(self, dt: datetime.datetime,
                              prices: dict[tuple[Asset, Exchange], float],
                              # exchange_name: str,
                              # handle_non_market_minutes: bool = False
                              ):
        if not self.positions:
            return
        self._dirty_stats = True

        for (asset_sid, exchange), last_sale_price in prices.items():
            exchange_positions = self.positions_by_exchange.get((exchange.name,))
            if not exchange_positions:
                continue
            for position in exchange_positions:
                if position.asset.sid != asset_sid:
                    continue
                if last_sale_price is None:
                    self._logger.warning(
                        f"Error updating last sale price for {position.asset.asset_name} on {dt}. Price is None")
                else:
                    position.last_sale_price = last_sale_price
                    position.last_sale_date = dt
        # for position in self.positions[()].values():
        #     # print("SYNCING")
        #     #last_sale_price = (await get_price(position.asset))["close"][0]
        #     # last_sale_price = prices.filter(pl.col("sid") == position.asset.sid)["close"][0]
        #     last_sale_price = prices.get((position.asset, position.exchange))
        #
        #     print(f"Last sale price for {position.asset.asset_name} is {last_sale_price} at {dt}")
        #     # inline ~isnan because this gets called once per position per minute
        #     if last_sale_price is None:
        #         self._logger.warning(
        #             f"Error updating last sale price for {position.asset.asset_name} on {dt}. Price is None")
        #     else:  # last_sale_price == last_sale_price:
        #         position.last_sale_price = last_sale_price
        #         position.last_sale_date = dt

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
            active_positions = self.get_position_list()
            calculate_position_tracker_stats(active_positions, position_count=len(active_positions),
                                             stats=self._stats, bond_book=self.bond_book)
            self._dirty_stats = False

        return self._stats
