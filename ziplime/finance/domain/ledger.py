import dataclasses
import datetime
import math
from collections import OrderedDict, deque

import numpy as np
import pandas as pd
import structlog

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.finance.bonds import BondBook
from ziplime.finance.margin import (
    FuturesMarginModel, NoFuturesMarginModel, margin_currency_of,
)
from ziplime.domain.account import Account
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.exchanges.repositories.exchange_repository import ExchangeRepository
from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.commission import Commission

from ziplime.finance.domain.order import Order
from ziplime.finance.domain.position_tracker import PositionTracker
from ziplime.finance.domain.transaction import Transaction
from ziplime.trading.domain.lot import Lot


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
                 data_frequency: datetime.timedelta,
                 futures_margin_model: FuturesMarginModel | None = None,
                 bond_book: BondBook | None = None,
                 ):
        if len(trading_sessions):
            start = trading_sessions[0]
        else:
            start = None
        # Have some fields of the portfolio changed? This should be accessed
        # through ``self._dirty_portfolio``
        self.__dirty_portfolio = False

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
        self._portfolio = Portfolio(start_date=start,
                                    starting_cash=0.00,
                                    portfolio_value=0.00,
                                    cash=0.00,
                                    cash_flow=0.00,
                                    pnl=0.00,
                                    returns=0.00,
                                    positions_value=0.00,
                                    positions_exposure=0.00,
                                    )

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

        # Coupon and amortization schedules, shared with the position tracker so that a
        # transaction, a mark and a coupon all read the same face value.
        self.bond_book = bond_book if bond_book is not None else BondBook()

        self.position_tracker = PositionTracker(data_frequency=data_frequency,
                                                bond_book=self.bond_book)

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

        self._buy_lots_by_asset: dict[Asset, deque[Lot]] = {}

        #: Bonds already redeemed, so a matured position is never repaid twice.
        self._redeemed_bonds: set[int] = set()

        #: Last session bond events were processed for; the schedule is read for the interval
        #: since it, so a record date on a non-trading day is not skipped over.
        self._last_bond_event_session: datetime.date | None = None

        # Always present, so "margin is not modelled" is a stated choice rather than an omission.
        self.futures_margin_model = futures_margin_model or NoFuturesMarginModel()

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

    def sync_last_sale_prices(self, dt: datetime.datetime, prices: dict[tuple[Asset, Exchange], float]):
        self.position_tracker.sync_last_sale_prices(
            dt=dt,
            prices=prices,
            # handle_non_market_minutes=handle_non_market_minutes,
            # exchange_name=self.default_exchange.name
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
        # `asset` is the exchange listing; the instrument itself hangs off `.asset`.
        if isinstance(asset.asset, FuturesContract):
            if not self.futures_margin_model.models_margin:
                self.futures_margin_model.warn_once()
            try:
                old_price = self._payout_last_sale_prices[asset]
            except KeyError:
                self._payout_last_sale_prices[asset] = transaction.price
            else:
                position = self.position_tracker.get_position(asset=asset)
                amount = position.amount if position is not None else 0
                price = transaction.price

                self._cash_flow(
                    self._calculate_payout(
                        asset.asset.multiplier,
                        amount,
                        old_price,
                        price,
                    ),
                )

                if amount + transaction.amount == 0:
                    del self._payout_last_sale_prices[asset]
                else:
                    self._payout_last_sale_prices[asset] = price
        elif isinstance(asset.asset, Bond):
            # A bond settles at its dirty price: the quote is a percentage of face value, and the
            # buyer additionally hands the seller the coupon accrued since the last payment. Both
            # corrections are needed -- the first is a factor of ten on a 1000 nominal, the second
            # averages half a coupon on every round trip.
            self._cash_flow(-(self.bond_price_in_money(asset=asset, quoted_price=transaction.price,
                                                       dt=transaction.dt) * transaction.amount))
        else:
            self._cash_flow(-(transaction.price * transaction.amount))
        # print("LEVERAGE: BEFORE EXCEUTION", self.account.leverage, self.account.net_leverage)

        self.position_tracker.execute_transaction(transaction)
        # print("LEVERAGE: AFTER EXCEUTION", self.account.leverage, self.account.net_leverage)

        # we only ever want the dict form from now on
        # transaction_dict = transaction.to_dict()
        try:
            # self._processed_transactions[transaction.dt].append(
            #     transaction_dict,
            # )
            self._processed_transactions[transaction.dt].append(
                transaction,
            )
        except KeyError:
            # self._processed_transactions[transaction.dt] = [transaction_dict]
            self._processed_transactions[transaction.dt] = [transaction]

        if asset not in self._buy_lots_by_asset:
            self._buy_lots_by_asset[asset] = deque()
        realized = 0.0
        realized_pnl_percentage = 0.00

        if transaction.amount > 0:
            self._buy_lots_by_asset[asset].append(
                Lot(quantity=transaction.amount, price=transaction.price, commission=transaction.commission or 0.00)
            )
            transaction.average_entry_price = transaction.price
        else:
            sell_qty = -transaction.amount
            sell_comm = transaction.commission or 0.00
            total_match_price = 0
            while sell_qty > 0 and self._buy_lots_by_asset[asset]:
                lot = self._buy_lots_by_asset[asset][0]
                match_qty = min(sell_qty, lot.quantity)

                pnl = (transaction.price - lot.price) * match_qty
                total_match_price += lot.price * match_qty
                realized += pnl

                lot.quantity -= match_qty
                sell_qty -= match_qty

                # Drop empty lot
                if lot.quantity == 0:
                    self._buy_lots_by_asset[asset].popleft()
            transaction.average_entry_price = total_match_price/-transaction.amount
            realized -= sell_comm
            if total_match_price > 0:
                realized_pnl_percentage = (realized / total_match_price) * 100
        transaction.realized_pnl = realized
        transaction.realized_pnl_percentage = realized_pnl_percentage

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

    def process_commission(self, commission: CommissionModel, tr):
        """Process the commission.

        Parameters
        ----------
        commission : CommissionModel
            The commission being paid.
        """
        # print(f"Commission for {asset.asset_name} is {cost}", tr.account.leverage, tr.account.net_leverage)
        self.position_tracker.handle_commission(asset=commission.asset, cost=commission.amount)
        # print(f"Commission 2 for {asset.asset_name} is {cost}", tr.account.leverage, tr.account.net_leverage)
        self._cash_flow(-commission.amount)
        # print(f"Commission 3 for {asset.asset_name} is {cost}", tr.account.leverage, tr.account.net_leverage)

    def close_position(self, asset: ExchangeAsset, dt: datetime.datetime):
        """Force-close a position whose contract has reached its auto close date.

        For a physically delivered contract this is the step that keeps the backtest honest: the
        position is being closed precisely so that it does not become a delivery obligation, and
        that is worth saying rather than letting it look like ordinary housekeeping.
        """
        instrument = getattr(asset, "asset", None)
        if getattr(instrument, "is_deliverable", False):
            self.logger.warning(
                "Closing a deliverable futures position to avoid delivery",
                symbol=asset.symbol, notice_date=str(getattr(instrument, "notice_date", None)),
                expiration_date=str(getattr(instrument, "expiration_date", None)), dt=str(dt))
        txn = self.position_tracker.maybe_create_close_position_transaction(
            asset=asset,
            dt=dt,
        )
        if txn is not None:
            self.process_transaction(transaction=txn)

    def bond_price_in_money(self, asset: ExchangeAsset, quoted_price: float, dt) -> float:
        """What one bond of ``asset`` changes hands for at ``quoted_price``: clean value plus НКД."""
        return self.bond_book.dirty_value(asset.asset, quoted_price, dt)

    def held_bonds(self) -> list[Bond]:
        """Every distinct bond currently held, across exchanges and accounts."""
        seen: dict[int, Bond] = {}
        for position in self.position_tracker.get_position_list():
            instrument = position.asset.asset
            if isinstance(instrument, Bond):
                seen.setdefault(instrument.id, instrument)
        return list(seen.values())

    async def process_bond_events(self, next_session, asset_service) -> None:
        """Earn and pay the bond schedule for ``next_session``.

        Mirrors :meth:`process_dividends`: a payment is *earned* when its record date arrives --
        fixing the entitlement against the position held that day -- and *paid* on the payment
        date. A coupon is therefore not lost by selling in between, which is what a record date
        means, and a short position is charged for one.

        Amortization instalments ride the same path: they are a payment per bond like a coupon,
        and the face value they leave behind comes from the stored schedule, so the quote of an
        amortizing issue is read against the right nominal from the next session on.
        """
        session_date = (next_session.date() if isinstance(next_session, datetime.datetime)
                        else next_session)
        # Everything since the previous session, so a record date that fell on a weekend or an
        # exchange holiday is still picked up on the next session the simulation runs.
        previous = self._last_bond_event_session
        if previous is None or previous >= session_date:
            previous = session_date - datetime.timedelta(days=1)
        self._last_bond_event_session = session_date

        held = self.held_bonds()
        if held:
            await self.bond_book.load(asset_service, held)
            earned = [event for bond in held
                      for event in self.bond_book.payments_entitled_between(
                          bond, after=previous, through=session_date)]
            if earned:
                self.position_tracker.earn_bond_payments(earned)

        payment = self.position_tracker.pay_bond_payments(session_date)
        if payment != 0:
            self._cash_flow(payment)

    async def redeem_matured_bonds(self, session, asset_service=None) -> None:
        """Repay the principal of every bond position that has reached maturity.

        Redemption is booked as a closing trade at par rather than left to the auto-close path,
        which would liquidate the position at whatever the last bar happened to print. A matured
        bond does not trade -- the issuer repays the outstanding face value -- and a backtest that
        marks it at a stale quote reports a loss or gain that never happened.
        """
        session_date = session.date() if isinstance(session, datetime.datetime) else session
        positions = [position for position in self.position_tracker.get_position_list()
                     if isinstance(position.asset.asset, Bond)]
        if not positions:
            return
        if asset_service is not None:
            await self.bond_book.load(asset_service,
                                      [position.asset.asset for position in positions])

        for position in positions:
            bond = position.asset.asset
            if bond.maturity_date is None or bond.maturity_date > session_date:
                continue
            if position.amount == 0 or bond.id in self._redeemed_bonds:
                continue
            # The principal outstanding on the maturity date itself: every amortization instalment
            # already paid has reduced it.
            face = self.bond_book.face_value(bond, bond.maturity_date)
            # Redemption is par *against what is left*, so the quote is measured against the same
            # outstanding nominal the settlement will convert it back with. Quoting it against the
            # nominal at issue would apply the amortization twice and repay a fraction of a
            # fraction.
            quoted = bond.price_quotation.quoted_price(face, face)
            self._redeemed_bonds.add(bond.id)
            self.logger.info("Redeeming a matured bond at par", symbol=position.asset.symbol,
                             maturity_date=str(bond.maturity_date), face_value=face,
                             amount=position.amount)
            self.process_transaction(Transaction(
                id=f"redeem-{position.asset.sid}-{bond.maturity_date}",
                asset=position.asset,
                amount=-position.amount,
                dt=session if isinstance(session, datetime.datetime) else
                datetime.datetime.combine(session_date, datetime.time.min,
                                          tzinfo=datetime.timezone.utc),
                price=quoted,
                order_id=None,
                exchange_name=position.exchange_name,
                trading_account_id=position.trading_account_id,
            ))

    async def process_dividends(self, next_session, asset_service):
        """Process dividends for the next session.

        This will earn us any dividends whose ex-date is the next session as
        well as paying out any dividends whose pay-date is the next session
        """
        position_tracker = self.position_tracker

        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        held_sids = set(position_tracker.positions)
        held_assets = [pos.asset.asset for pos in self.position_tracker.get_position_list()]
        if held_sids:
            cash_dividends = await asset_service.get_cash_dividends_with_ex_date(
                assets=held_assets, date=next_session  # self.data_bundle.asset_repository
            )
            # stock_dividends = await asset_service.get_stock_dividends_with_ex_date(
            #     assets=held_assets, date=next_session  # self.data_bundle.asset_repository
            # )
            stock_dividends = []

            # Earning a dividend just marks that we need to get paid out on
            # the dividend's pay-date. This does not affect our cash yet.
            position_tracker.earn_dividends(
                cash_dividends=cash_dividends,
                stock_dividends=stock_dividends,
            )

        # Pay out the dividends whose pay-date is the next session. This does
        # affect out cash.
        dividends = position_tracker.pay_dividends(next_session)
        if dividends > 0:
            self._cash_flow(dividends)

    async def capital_change(self, change_amount: float):
        # `update_portfolio` is a coroutine. Called without awaiting, the portfolio value a target
        # capital change is measured against was whatever the last bar left behind, so the deposit
        # or withdrawal computed from it was wrong by a bar of profit and loss.
        await self.update_portfolio()
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
            return [o for o in self._orders_by_id.values()]

        return [o for o in self._orders_by_modified.get(dt, {}).values()]

    @property
    def positions(self):
        """Snapshots of the currently open positions.

        Copies, not the live objects: performance packets keep whatever this returns, and handing
        out live positions made already-recorded sessions change as the simulation went on -- a
        contract closed in September silently showed as flat in June's record.
        """
        return [dataclasses.replace(position)
                for position in self.position_tracker.get_position_list()]

    def futures_margin_by_currency(self, maintenance: bool = False) -> dict[str, float]:
        """Margin the open futures positions tie up, keyed by the currency it is posted in.

        Margin currency is a property of the exchange, not of the quote: an exchange may collect
        its local currency even for the contracts it quotes in dollars. Amounts in different currencies are reported
        separately because adding them would need an FX rate the ledger does not carry.
        """
        model = self.futures_margin_model
        totals: dict[str, float] = {}
        for position in self.position_tracker.get_position_list():
            if not isinstance(position.asset.asset, FuturesContract):
                continue
            margin = (model.maintenance_margin if maintenance else model.initial_margin)
            amount = margin(position.asset, position.amount, position.last_sale_price)
            currency = margin_currency_of(position.asset)
            totals[currency] = totals.get(currency, 0.0) + amount
        return totals

    def futures_margin_requirement(self, maintenance: bool = False,
                                   currency: str | None = None) -> float:
        """Margin posted in a single currency.

        Args:
            maintenance: Report maintenance margin rather than initial.
            currency: Which currency to report. Required when the book posts margin in more than
                one; use :meth:`futures_margin_by_currency` to see them all.

        Returns 0.0 under :class:`~ziplime.finance.margin.NoFuturesMarginModel`; check
        ``futures_margin_model.models_margin`` to tell that from a genuinely unmargined book.

        Raises:
            ValueError: if the book spans several margin currencies and none was named.
        """
        totals = self.futures_margin_by_currency(maintenance=maintenance)
        if currency is not None:
            return totals.get(currency, 0.0)
        if len(totals) > 1:
            raise ValueError(
                f"Futures margin is posted in more than one currency ({', '.join(sorted(totals))})"
                f" and they cannot be added without an FX rate. Pass currency=..., or call "
                f"futures_margin_by_currency()."
            )
        return next(iter(totals.values()), 0.0)

    def _get_payout_total(self):
        """Accrue variation margin on open futures positions since the last time this ran.

        Futures settle daily rather than carrying a position value, so the ledger tracks the price
        each position was last marked at and turns the change into cash.
        """
        total = 0
        for asset, old_price in list(self._payout_last_sale_prices.items()):
            position = self.position_tracker.get_position(asset=asset)
            if position is None:
                # The position was closed without going through process_transaction (an expired
                # contract with no price to close at, say). There is nothing left to settle.
                self._payout_last_sale_prices.pop(asset, None)
                continue
            self._payout_last_sale_prices[asset] = price = position.last_sale_price
            amount = position.amount
            total += self._calculate_payout(
                asset.asset.multiplier,
                amount,
                old_price,
                price,
            )

        return total

    def synchronize_exchange_portfolio(self, portfolio: Portfolio):
        # start_cash = sum(exchange.get_start_cash_balance() for exchange in exchange_repository.get_all_exchanges())

        pt = self.position_tracker
        for asset, position in portfolio.positions.items():
            pt.update_position(
                asset=asset, exchange_name=position.exchange_name,
                last_sale_price=position.last_sale_price,
                last_sale_date=position.last_sale_date,
                cost_basis=position.cost_basis,
                amount=position.amount,
                trading_account_id=position.trading_account_id,
            )
        self._portfolio.cash = portfolio.cash
        self._portfolio.starting_cash = portfolio.starting_cash
        self._portfolio.portfolio_value = portfolio.portfolio_value

        self._portfolio.positions = pt.get_positions()
        position_stats = pt.stats

        self._portfolio.positions_value = position_value = position_stats.net_value
        self._portfolio.positions_exposure = position_stats.net_exposure
        payout_total = self._get_payout_total()
        if payout_total != 0:
            # Variation margin settles in both directions. Applying only gains -- while
            # _get_payout_total has already advanced each position's mark to the new price --
            # dropped every losing day on the floor, so a long futures position could not lose.
            self._cash_flow(payout_total)

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

    async def update_portfolio(self) -> None:
        """Force a computation of the current portfolio state."""
        if not self._dirty_portfolio:
            return

        pt = self.position_tracker

        self._portfolio.positions = pt.get_positions()
        position_stats = pt.stats

        self._portfolio.positions_value = position_value = position_stats.net_value
        self._portfolio.positions_exposure = position_stats.net_exposure
        payout_total = self._get_payout_total()
        if payout_total != 0:
            # Variation margin settles in both directions. Applying only gains -- while
            # _get_payout_total has already advanced each position's mark to the new price --
            # dropped every losing day on the floor, so a long futures position could not lose.
            self._cash_flow(payout_total)

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
        # self.update_portfolio()
        return self._portfolio

    def calculate_period_stats(self):
        position_stats = self.position_tracker.stats
        portfolio_value = self.portfolio.portfolio_value

        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = position_stats.gross_exposure / portfolio_value
            net_leverage = position_stats.net_exposure / portfolio_value
        # if gross_leverage > 5:
        #     print("a")
        return portfolio_value, gross_leverage, net_leverage

    def update_account(self):
        if not self._dirty_account:
            return

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

    @property
    def account(self):
        # self.update_account()
        return self._account
