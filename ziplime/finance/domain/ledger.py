import datetime
import math
from collections import OrderedDict

import numpy as np
import pandas as pd
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.commission import CommissionModel

from ziplime.finance.domain.order import Order
from ziplime.finance.domain.position_tracker import PositionTracker
from ziplime.finance.domain.transaction import Transaction


class Ledger:
    """The ledger tracks all orders and transactions as well as the current
    state of the portfolio and positions.

    Attributes
    ----------
    portfolio : ziplime.protocol.Portfolio
        The updated portfolio being managed.
    account : ziplime.protocol.Account
        The updated account being managed.
    position_tracker : PositionTracker
        The current set of positions.
    todays_returns : float
        The current day's returns. In minute emission mode, this is the partial
        day's returns. In daily emission mode, this is
        ``daily_returns[session]``.
    daily_returns_series : pd.Series
        The daily returns series. Days that have not yet finished will hold
        a value of ``np.nan``.
    daily_returns_array : np.ndarray
        The daily returns as an ndarray. Days that have not yet finished will
        hold a value of ``np.nan``.
    """

    def __init__(self, trading_sessions: pd.DatetimeIndex,
                 exchanges: dict[str, Exchange],
                 data_frequency: datetime.timedelta):
        if len(trading_sessions):
            start = trading_sessions[0]
        else:
            start = None
        # Have some fields of the portfolio changed? This should be accessed
        # through ``self._dirty_portfolio``
        self.__dirty_portfolio = False

        first_exchange = exchanges[list(exchanges.keys())[0]]
        self.default_exchange = first_exchange
        self._portfolio = Portfolio(start_date=start,
                                    starting_cash=self.default_exchange.get_start_cash_balance(),
                                    portfolio_value=self.default_exchange.get_start_cash_balance(),
                                    cash=self.default_exchange.get_current_cash_balance(),
                                    cash_flow=0.00,
                                    pnl=0.00,
                                    returns=0.00,
                                    positions_value=0.00,
                                    positions_exposure=0.00,
                                    )

        self.logger = structlog.get_logger(__name__)

        self.daily_returns_series = pd.Series(
            np.nan,
            index=trading_sessions,
        )
        # Get a view into the storage of the returns series. Metrics
        # can access this directly in minute mode for performance reasons.
        self.daily_returns_array = self.daily_returns_series.values

        self._previous_total_returns = 0

        # this is a component of the cache key for the account
        self._position_stats = None

        # Have some fields of the account changed?
        self._dirty_account = True
        # self._immutable_account =
        self._account = Account(
            settled_cash=0.00,
            accrued_interest=0.00,
            buying_power=math.inf,
            equity_with_loan=0.00,
            total_positions_value=0.00,
            total_positions_exposure=0.00,
            regt_equity=0.00,
            regt_margin=math.inf,
            initial_margin_requirement=0.00,
            maintenance_margin_requirement=0.00,
            available_funds=0.00,
            excess_liquidity=0.00,
            cushion=0.00,
            day_trades_remaining=math.inf,
            leverage=0.00,
            net_leverage=0.00,
            net_liquidation=0.00
        )

        # The exchange blotter can override some fields on the account. This is
        # way to tangled up at the moment but we aren't fixing it today.
        self._account_overrides = {}
        self._data_frequency = data_frequency

        self.position_tracker = PositionTracker(exchanges=exchanges, data_frequency=data_frequency)

        self._processed_transactions = {}

        self._orders_by_modified = {}
        self._orders_by_id = OrderedDict()

        # Keyed by asset, the previous last sale price of positions with
        # payouts on price differences, e.g. Futures.
        #
        # This dt is not the previous minute to the minute for which the
        # calculation is done, but the last sale price either before the period
        # start, or when the price at execution.
        self._payout_last_sale_prices = {}

    @property
    def todays_returns(self) -> float:
        # compute today's returns in returns space instead of portfolio-value
        # space to work even when we have capital changes
        return (self.portfolio.returns + 1) / (self._previous_total_returns + 1) - 1

    @property
    def _dirty_portfolio(self):
        return self.__dirty_portfolio

    @_dirty_portfolio.setter
    def _dirty_portfolio(self, value):
        if value:
            # marking the portfolio as dirty also marks the account as dirty
            self.__dirty_portfolio = self._dirty_account = value
        else:
            self.__dirty_portfolio = value

    def start_of_session(self, session_label):
        self._processed_transactions.clear()
        self._orders_by_modified.clear()
        self._orders_by_id.clear()

        # Save the previous day's total returns so that ``todays_returns``
        # produces returns since yesterday. This does not happen in
        # ``end_of_session`` because we want ``todays_returns`` to produce the
        # correct value in metric ``end_of_session`` handlers.
        self._previous_total_returns = self.portfolio.returns

    def end_of_bar(self, session_ix):
        # make daily_returns hold the partial returns, this saves many
        # metrics from doing a concat and copying all of the previous
        # returns
        if isinstance(self.daily_returns_array, np.ndarray):
            self.daily_returns_array[session_ix] = self.todays_returns
        elif isinstance(self.daily_returns_array, pd.Series):
            self.daily_returns_array.iloc[session_ix] = self.todays_returns
        else:
            raise ValueError("Unknown daily returns array type")

    def end_of_session(self, session_ix: int):
        # save the daily returns time-series
        self.daily_returns_series.iloc[session_ix] = self.todays_returns

    def sync_last_sale_prices(self, dt: datetime.datetime, handle_non_market_minutes: bool = False):
        self.position_tracker.sync_last_sale_prices(
            dt=dt,
            handle_non_market_minutes=handle_non_market_minutes,
            exchange_name=self.default_exchange.name
        )
        self._dirty_portfolio = True

    @staticmethod
    def _calculate_payout(multiplier: float, amount: float, old_price: float, price: float) -> float:
        return (price - old_price) * multiplier * amount

    def _cash_flow(self, amount: float) -> None:
        self._dirty_portfolio = True
        self._portfolio.cash_flow += amount
        self._portfolio.cash += amount

    def process_transaction(self, transaction: Transaction):
        """Add a transaction to ledger, updating the current state as needed.

        Parameters
        ----------
        transaction : Transaction
            The transaction to execute.
        """
        asset = transaction.asset
        if isinstance(asset, FuturesContract):
            try:
                old_price = self._payout_last_sale_prices[asset]
            except KeyError:
                self._payout_last_sale_prices[asset] = transaction.price
            else:
                position = self.position_tracker.positions[asset]
                amount = position.amount
                price = transaction.price

                self._cash_flow(
                    self._calculate_payout(
                        asset.price_multiplier,
                        amount,
                        old_price,
                        price,
                    ),
                )

                if amount + transaction.amount == 0:
                    del self._payout_last_sale_prices[asset]
                else:
                    self._payout_last_sale_prices[asset] = price
        else:
            self._cash_flow(-(transaction.price * transaction.amount))

        self.position_tracker.execute_transaction(transaction)

        # we only ever want the dict form from now on
        transaction_dict = transaction.to_dict()
        try:
            self._processed_transactions[transaction.dt].append(
                transaction_dict,
            )
        except KeyError:
            self._processed_transactions[transaction.dt] = [transaction_dict]

    def process_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.

        Parameters
        ----------
        splits: list[(Asset, float)]
            A list of splits. Each split is a tuple of (asset, ratio).
        """
        leftover_cash = self.position_tracker.handle_splits(splits)
        if leftover_cash > 0:
            self._cash_flow(leftover_cash)

    def process_order(self, order: Order):
        """Keep track of an order that was placed.

        Parameters
        ----------
        order : Order
            The order to record.
        """
        try:
            dt_orders = self._orders_by_modified[order.dt]
        except KeyError:
            self._orders_by_modified[order.dt] = OrderedDict(
                [
                    (order.id, order),
                ]
            )
            self._orders_by_id[order.id] = order
        else:
            self._orders_by_id[order.id] = dt_orders[order.id] = order
            # to preserve the order of the orders by modified date
            dt_orders.move_to_end(order.id, last=True)

        self._orders_by_id.move_to_end(order.id, last=True)

    def process_commission(self, commission: CommissionModel):
        """Process the commission.

        Parameters
        ----------
        commission : CommissionModel
            The commission being paid.
        """
        asset = commission["asset"]
        cost = commission["cost"]

        self.position_tracker.handle_commission(asset, cost)
        self._cash_flow(-cost)

    def close_position(self, asset: Asset, dt: datetime.datetime):
        txn = self.position_tracker.maybe_create_close_position_transaction(
            asset=asset,
            dt=dt,
        )
        if txn is not None:
            self.process_transaction(transaction=txn)

    async def process_dividends(self, next_session, adjustment_reader):
        """Process dividends for the next session.

        This will earn us any dividends whose ex-date is the next session as
        well as paying out any dividends whose pay-date is the next session
        """
        position_tracker = self.position_tracker

        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        held_sids = set(position_tracker.positions)
        if held_sids:
            cash_dividends = adjustment_reader.get_dividends_with_ex_date(
                assets=held_sids, date=next_session  # self.data_bundle.asset_repository
            )
            stock_dividends = await adjustment_reader.get_stock_dividends_with_ex_date(
                assets=held_sids, date=next_session  # self.data_bundle.asset_repository
            )

            # Earning a dividend just marks that we need to get paid out on
            # the dividend's pay-date. This does not affect our cash yet.
            position_tracker.earn_dividends(
                cash_dividends=cash_dividends,
                stock_dividends=stock_dividends,
            )

        # Pay out the dividends whose pay-date is the next session. This does
        # affect out cash.
        self._cash_flow(
            position_tracker.pay_dividends(
                next_session,
            ),
        )

    def capital_change(self, change_amount: float):
        self.update_portfolio()
        # we update the cash and total value so this is not dirty
        self._portfolio.portfolio_value += change_amount
        self._portfolio.cash += change_amount

    def transactions(self, dt=None):
        """Retrieve the dict-form of all of the transactions in a given bar or
        for the whole simulation.

        Parameters
        ----------
        dt : datetime.datetime or None, optional
            The particular datetime to look up transactions for. If not passed,
            or None is explicitly passed, all of the transactions will be
            returned.

        Returns
        -------
        transactions : list[dict]
            The transaction information.
        """
        if dt is None:
            # flatten the by-day transactions
            return [
                txn
                for by_day in self._processed_transactions.values()
                for txn in by_day
            ]

        return self._processed_transactions.get(dt, [])

    def orders(self, dt=None):
        """Retrieve the dict-form of all of the orders in a given bar or for
        the whole simulation.

        Parameters
        ----------
        dt : datetime.datetime or None, optional
            The particular datetime to look up order for. If not passed, or
            None is explicitly passed, all of the orders will be returned.

        Returns
        -------
        orders : list[dict]
            The order information.
        """
        if dt is None:
            # orders by id is already flattened
            return [o.to_dict() for o in self._orders_by_id.values()]

        return [o.to_dict() for o in self._orders_by_modified.get(dt, {}).values()]

    @property
    def positions(self):
        return self.position_tracker.get_position_list()

    def _get_payout_total(self, positions):
        calculate_payout = self._calculate_payout
        payout_last_sale_prices = self._payout_last_sale_prices

        total = 0
        for asset, old_price in payout_last_sale_prices.items():
            position = positions[asset]
            payout_last_sale_prices[asset] = price = position.last_sale_price
            amount = position.amount
            total += calculate_payout(
                asset.price_multiplier,
                amount,
                old_price,
                price,
            )

        return total

    def update_portfolio(self) -> None:
        """Force a computation of the current portfolio state."""
        if not self._dirty_portfolio:
            return

        pt = self.position_tracker

        self._portfolio.positions = pt.get_positions()
        position_stats = pt.stats

        self._portfolio.positions_value = position_value = position_stats.net_value
        self._portfolio.positions_exposure = position_stats.net_exposure
        self._cash_flow(self._get_payout_total(pt.positions))

        start_value = self._portfolio.portfolio_value

        # update the new starting value
        self._portfolio.portfolio_value = end_value = self._portfolio.cash + position_value

        pnl = end_value - start_value
        if start_value != 0:
            returns = pnl / start_value
        else:
            returns = 0.00

        self._portfolio.pnl += pnl
        self._portfolio.returns = (1 + self._portfolio.returns) * (1 + returns) - 1

        # the portfolio has been fully synced
        self._dirty_portfolio = False

    @property
    def portfolio(self):
        """Compute the current portfolio.

        Notes
        -----
        This is cached, repeated access will not recompute the portfolio until
        the portfolio may have changed.
        """
        self.update_portfolio()
        return self._portfolio

    def calculate_period_stats(self):
        position_stats = self.position_tracker.stats
        portfolio_value = self.portfolio.portfolio_value

        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = position_stats.gross_exposure / portfolio_value
            net_leverage = position_stats.net_exposure / portfolio_value

        return portfolio_value, gross_leverage, net_leverage

    @property
    def account(self):
        if self._dirty_account:
            portfolio = self.portfolio

            account = self._account

            # If no attribute is found in the ``_account_overrides`` resort to
            # the following default values. If an attribute is found use the
            # existing value. For instance, a exchange may provide updates to
            # these attributes. In this case we do not want to over write the
            # exchange values with the default values.
            account.settled_cash = portfolio.cash
            account.accrued_interest = 0.00
            account.buying_power = np.inf
            account.equity_with_loan = portfolio.portfolio_value
            account.total_positions_value = portfolio.portfolio_value - portfolio.cash
            account.total_positions_exposure = portfolio.positions_exposure
            account.regt_equity = portfolio.cash
            account.regt_margin = np.inf
            account.initial_margin_requirement = 0.00
            account.maintenance_margin_requirement = 0.00
            account.available_funds = portfolio.cash
            account.excess_liquidity = portfolio.cash
            account.cushion = (
                (portfolio.cash / portfolio.portfolio_value)
                if portfolio.portfolio_value
                else np.nan
            )
            account.day_trades_remaining = np.inf
            (
                account.net_liquidation,
                account.gross_leverage,
                account.net_leverage,
            ) = self.calculate_period_stats()

            account.leverage = account.gross_leverage

            # apply the overrides
            for k, v in self._account_overrides.items():
                setattr(account, k, v)

            # the account has been fully synced
            self._dirty_account = False

        return self._account
