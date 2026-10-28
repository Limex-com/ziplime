import datetime
import math
import uuid
from collections import OrderedDict, deque
from collections.abc import Iterable, Mapping
from dataclasses import fields, replace
from copy import copy

import numpy as np
import pandas as pd
import structlog
from iso4217 import Currency as ISOCurrency

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.bonds import BondBook
from ziplime.finance.margin import (
    FuturesMarginModel, NoFuturesMarginModel, margin_currency_of,
)
from ziplime.domain.account import Account
from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.exchanges.exchange import Exchange
from ziplime.finance.domain.commission import Commission

from ziplime.finance.domain.order import Order
from ziplime.finance.domain.position_tracker import PositionTracker
from ziplime.finance.domain.transaction import Transaction
from ziplime.trading.domain.lot import Lot
from ziplime.finance.domain.cash_event import CashEvent, CashKey
from ziplime.finance.domain.cash_event_kind import CashEventKind
from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.domain.investment_event_kind import InvestmentEventKind
from ziplime.finance.domain.investment_history import InvestmentHistory
from ziplime.finance.domain.investment_snapshot import InvestmentSnapshot
from ziplime.finance.domain.investment_position import InvestmentPosition
from ziplime.finance.monetary_valuation_service import MonetaryValuationService
from ziplime.assets.domain.bond_event_type import BondEventType


def _is_margined_option(instrument) -> bool:
    """Whether ``instrument`` is an option that settles variation margin rather than a premium.

    A free function rather than a method because three separate places have to agree on it -- the
    cash flow at a trade, the value of the position, and the margin it ties up -- and a
    disagreement between any two of them double-counts or loses the position entirely.
    """
    return isinstance(instrument, OptionContract) and instrument.premium_style.is_margined


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
    reporting_base_currency : str or None
        ISO code for execution-book money when currency reporting is enabled.
    reporting_events : list[tuple[datetime.datetime, float, bool]]
        Aware posting time, base-currency cash-flow amount, and external-capital
        flag for plain base-currency reporting. Native currency reports read
        dated amounts directly from the ledger's postings.
    native_currency_accounting : bool
        Whether native scoped cash and historical monetary accounting are initialized.
    _reporting_dt : datetime.datetime or None
        Current aware posting/valuation time while reporting is enabled.
    _investment_base : str or None
        Accounting currency for aggregating investment-mode portfolio/account totals.
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

        self._buy_lots_by_asset: dict[tuple[str, str, ExchangeAsset], deque[Lot]] = {}

        #: Bonds already redeemed, so a matured position is never repaid twice.
        self._redeemed_bonds: set[int] = set()

        #: Last session bond events were processed for; the schedule is read for the interval
        #: since it, so a record date on a non-trading day is not skipped over.
        self._last_bond_event_session: datetime.date | None = None

        # Always present, so "margin is not modelled" is a stated choice rather than an omission.
        self.futures_margin_model = futures_margin_model or NoFuturesMarginModel()
        self.reporting_base_currency: str | None = None
        self.reporting_events: list[tuple[datetime.datetime, float, bool]] = []
        self._reporting_dt: datetime.datetime | None = None
        self.native_currency_accounting = False
        self._investment_base: str | None = None
        self._postings: list[Transaction | InvestmentEvent] = []
        self._posting_identities: dict[tuple[InvestmentKey, str], Transaction | InvestmentEvent] = {}
        self._accounting_keys: dict[InvestmentKey, None] = {}
        self._fill_opening: dict[tuple[InvestmentKey, str], tuple[float, float]] = {}
        self._money_flows: dict[str, dict[InvestmentKey, list[tuple[datetime.datetime, str, float]]]] = {
            field: {} for field in ("realized", "income", "fees", "cash_flow")}
        self._currency_totals: dict[str, dict[str, dict[InvestmentKey, float]]] = {}
        self._opening_posting_count = 0
        self._cash_flow_baselines: dict[str, float] = {}
        self._cash_events: dict[tuple[CashKey, str], CashEvent] = {}
        self._restoring_history = False
        self._monetary_valuation: MonetaryValuationService | None = None
        self._currency_conversion: CurrencyConversionService | None = None

    @property
    def transaction_history(self) -> tuple[Transaction, ...]:
        """All executed fills; session reporting indexes reference these same objects."""
        return tuple(posting for posting in self._postings if isinstance(posting, Transaction))

    @property
    def investment_events(self) -> tuple[InvestmentEvent, ...]:
        """Compatibility export; trade DTOs are constructed only when requested."""
        return tuple(self._history_event(posting) for posting in self._postings)

    @staticmethod
    def _history_event(posting: Transaction | InvestmentEvent) -> InvestmentEvent:
        if isinstance(posting, Transaction):
            return InvestmentEvent(
                id=posting.id, dt=posting.dt, kind=InvestmentEventKind.TRADE, key=posting.key,
                currency=posting.asset.quote.asset_name, quantity=posting.amount,
                unit_value=posting.native_unit_value, quoted_price=posting.price, order_id=posting.order_id)
        return posting

    @property
    def investment_quantities(self) -> dict[InvestmentKey, float]:
        """FIFO quantities for reconciliation, derived from the ledger's one set of lots."""
        return {key: math.fsum(lot.quantity for lot in lots)
                for key, lots in self._buy_lots_by_asset.items() if not isinstance(key[2].asset, Currency)}

    @property
    def cash_events(self) -> tuple[CashEvent, ...]:
        return tuple(self._cash_events.values())

    def _add_total(self, field: str, key: InvestmentKey, flow: tuple[datetime.datetime, str, float]) -> None:
        """Retain a dated native amount and update already requested currency totals."""
        self._money_flows[field].setdefault(key, []).append(flow)
        stamp, native, amount = flow
        for currency in tuple(self._currency_totals):
            try:
                converted = amount * self._currency_conversion.rate(base=native, quote=currency, dt=stamp)
            except ValueError:
                # The execution remains recorded. A requested report must resolve its FX.
                del self._currency_totals[currency]
                continue
            totals = self._currency_totals[currency][field]
            totals[key] = totals.get(key, 0.0) + converted

    def _add_lot_cost(self, lot: Lot, flow: tuple[datetime.datetime, str, float]) -> None:
        lot.costs.append(flow)
        stamp, native, amount = flow
        for currency in tuple(self._currency_totals):
            try:
                converted = amount * self._currency_conversion.rate(base=native, quote=currency, dt=stamp)
            except ValueError:
                del self._currency_totals[currency]
                continue
            lot.cost_by_currency[currency] = lot.cost_by_currency.get(currency, 0.0) + converted

    def _ensure_currency(self, currency: str) -> dict[str, dict[InvestmentKey, float]]:
        """Initialize an additional currency from native amounts, without rematching FIFO."""
        if currency not in self._currency_totals:
            totals = {}
            for field, by_key in self._money_flows.items():
                totals[field] = {
                    key: math.fsum(amount * self._currency_conversion.rate(base=native, quote=currency, dt=stamp)
                                   for stamp, native, amount in flows)
                    for key, flows in by_key.items()}
            costs = [(lot, math.fsum(amount * self._currency_conversion.rate(
                base=native, quote=currency, dt=stamp) for stamp, native, amount in lot.costs))
                for lots in self._buy_lots_by_asset.values() for lot in lots]
            for lot, cost in costs:
                lot.cost_by_currency[currency] = cost
            self._currency_totals[currency] = totals
        return self._currency_totals[currency]

    def _match_transaction(self, transaction: Transaction) -> None:
        """Match native quantities once; dated monetary components serve every currency."""
        key = transaction.key
        lots = self._buy_lots_by_asset.setdefault(key, deque())
        quantity = transaction.amount
        if isinstance(transaction.asset.asset, Currency):
            self._fill_opening[(key, transaction.id)] = (0.0, 0.0)
            return
        quoted_cost = 0.0
        quoted_pnl = 0.0
        matched_quantity = 0.0
        while lots and quantity * lots[0].quantity < 0:
            lot = lots[0]
            matched = min(abs(quantity), abs(lot.quantity))
            direction = math.copysign(1.0, lot.quantity)
            fraction = matched / abs(lot.quantity)
            self._add_total("realized", key, (transaction.dt, transaction.asset.quote.asset_name,
                                            direction * matched * transaction.native_unit_value))
            for stamp, currency, amount in lot.costs:
                self._add_total("realized", key, (stamp, currency, -amount * fraction))
            lot.costs = [(stamp, currency, amount * (1 - fraction)) for stamp, currency, amount in lot.costs]
            for currency in lot.cost_by_currency:
                lot.cost_by_currency[currency] *= 1 - fraction
            if lot.price is not None and transaction.price is not None:
                quoted_cost += lot.price * matched
                quoted_pnl += direction * matched * (transaction.price - lot.price)
            matched_quantity += matched
            lot.quantity -= direction * matched
            quantity += direction * matched
            if math.isclose(lot.quantity, 0.0, abs_tol=1e-12):
                lots.popleft()
        self._fill_opening[(key, transaction.id)] = (abs(quantity), abs(quantity))
        if not math.isclose(quantity, 0.0, abs_tol=1e-12):
            lot = Lot(quantity=quantity, price=transaction.price, transaction_id=transaction.id)
            self._add_lot_cost(lot, (transaction.dt, transaction.asset.quote.asset_name,
                                     quantity * transaction.native_unit_value))
            lots.append(lot)
        transaction.average_entry_price = (quoted_cost / matched_quantity if matched_quantity
                                           else transaction.price or 0.0)
        transaction.realized_pnl = quoted_pnl - (transaction.commission or 0.0) if matched_quantity else 0.0
        transaction.realized_pnl_percentage = (100 * transaction.realized_pnl / quoted_cost if quoted_cost else 0.0)

    def _apply_fee(self, event: InvestmentEvent) -> None:
        key = event.key
        fill = self._posting_identities[(key, event.transaction_id)]
        opening, adjusted_opening = self._fill_opening[(key, event.transaction_id)]
        opening_fee = event.amount * opening / abs(fill.amount)
        lots = [lot for lot in self._buy_lots_by_asset.get(key, ()) if lot.transaction_id == event.transaction_id]
        remaining = math.fsum(abs(lot.quantity) for lot in lots)
        retained = opening_fee * remaining / adjusted_opening if adjusted_opening else 0.0
        for lot in lots:
            self._add_lot_cost(lot, (event.dt, event.currency, retained * abs(lot.quantity) / remaining))
        self._add_total("realized", key, (event.dt, event.currency, -(event.amount - retained)))
        self._add_total("fees", key, (event.dt, event.currency, event.amount))

    def _validate_posting(self, posting: Transaction | InvestmentEvent) -> bool:
        existing = self._posting_identities.get((posting.key, posting.id))
        if existing is not None:
            previous = self._history_event(existing)
            incoming = self._history_event(posting)
            # Older history exports did not carry the quoted price or order ID.
            same_fact = replace(previous, quoted_price=None, order_id=None) == replace(
                incoming, quoted_price=None, order_id=None)
            conflicting_metadata = any(
                left is not None and right is not None and left != right
                for left, right in ((previous.quoted_price, incoming.quoted_price),
                                    (previous.order_id, incoming.order_id)))
            if not same_fact or conflicting_metadata:
                raise ValueError(f"Conflicting investment event {posting.id}.")
            return False
        if self.native_currency_accounting:
            if posting.dt.tzinfo is None or posting.dt.utcoffset() is None:
                raise ValueError("Investment event timestamps must be timezone-aware.")
            if self._postings and posting.dt < self._postings[-1].dt:
                raise ValueError("Investment events must be recorded in chronological posting order.")
            if not posting.id or not all(posting.key[:2]):
                raise ValueError("Investment events need an identity and explicit account scope.")
        if isinstance(posting, Transaction):
            if not all(math.isfinite(value) for value in (posting.amount, posting.native_unit_value)):
                raise ValueError("Investment monetary values and quantities must be finite.")
            if self.native_currency_accounting and (posting.amount == 0 or posting.native_unit_value < 0):
                raise ValueError("An investment fill needs a nonzero quantity and nonnegative unit value.")
            if self.native_currency_accounting:
                self._monetary_valuation.unit_value(asset=posting.asset, price=0.0, dt=posting.dt)
        else:
            ISOCurrency(value=posting.currency)
            if not all(math.isfinite(value) for value in (posting.amount, posting.quantity, posting.unit_value)):
                raise ValueError("Investment monetary values and quantities must be finite.")
            if posting.kind == InvestmentEventKind.FEE:
                fill = self._posting_identities.get((posting.key, posting.transaction_id))
                if not isinstance(fill, Transaction):
                    raise ValueError("A fee must reference an existing scoped ledger fill; transaction_id required.")
            elif posting.kind == InvestmentEventKind.SPLIT:
                if posting.amount <= 0:
                    raise ValueError("Split ratio must be positive.")
                quantity = self.investment_quantities.get(posting.key, 0.0) / posting.amount
                if not math.isclose(quantity, round(quantity), abs_tol=1e-10):
                    raise ValueError("Fractional split cash-in-lieu requires an explicit disposal event.")
            elif posting.kind != InvestmentEventKind.INCOME:
                raise ValueError(f"Unsupported investment event kind: {posting.kind}.")
        return True

    def _record_posting(self, posting: Transaction | InvestmentEvent, check_funding: bool = False) -> bool:
        if not self._validate_posting(posting):
            return False
        if check_funding:
            changes = {}
            for key, amount in self._posting_cash_changes(posting):
                changes[key] = changes.get(key, 0.0) + amount
            self.validate_cash_changes(dt=posting.dt, changes=changes)
        self._postings.append(posting)
        self._posting_identities[(posting.key, posting.id)] = posting
        self._accounting_keys.setdefault(posting.key, None)
        if isinstance(posting, Transaction):
            self._match_transaction(posting)
        elif posting.kind == InvestmentEventKind.FEE:
            self._apply_fee(posting)
        elif posting.kind == InvestmentEventKind.INCOME:
            self._add_total("income", posting.key, (posting.dt, posting.currency, posting.amount))
        elif posting.kind == InvestmentEventKind.SPLIT:
            for lot in self._buy_lots_by_asset.get(posting.key, ()):
                lot.quantity /= posting.amount
                if lot.price is not None:
                    lot.price *= posting.amount
            for identity, (opening, adjusted) in self._fill_opening.items():
                if identity[0] == posting.key:
                    self._fill_opening[identity] = (opening, adjusted / posting.amount)
        for cash_key, amount in self._posting_cash_changes(posting):
            self._add_total("cash_flow", posting.key, (posting.dt, cash_key[2], amount))
        return True

    def record_investment_event(self, event: InvestmentEvent, *, check_funding: bool = True) -> bool:
        """Import/post a history DTO; trades become canonical Transaction records."""
        if event.kind == InvestmentEventKind.TRADE:
            if event.currency != event.key[2].quote.asset_name:
                raise ValueError("Fill consideration must be denominated in the listing's quote currency.")
            posting = Transaction(
                id=event.id, dt=event.dt, asset=event.key[2], amount=event.quantity,
                price=event.quoted_price, exchange_name=event.key[0], trading_account_id=event.key[1],
                order_id=event.order_id, native_unit_value=event.unit_value)
        else:
            posting = event
        return self._record_posting(posting, check_funding=check_funding)

    def record_execution(self, event: InvestmentEvent) -> bool:
        if event.kind not in (InvestmentEventKind.TRADE, InvestmentEventKind.FEE):
            raise ValueError("Only executed trades and linked fees can be recorded as executions.")
        return self.record_investment_event(event=event, check_funding=False)

    def restore_accounting_history(self, history: InvestmentHistory) -> None:
        """Import posted facts without rerunning pre-submission funding decisions."""
        if self._postings or self._cash_events:
            raise ValueError("Restore requires an empty ledger history.")
        self._restoring_history = True
        try:
            for event in history.events:
                self.record_investment_event(event, check_funding=False)
            for event in history.cash_events:
                self.record_cash(event)
            self.position_tracker.restore_income(income_due=history.income_due, posted=history.events)
        finally:
            self._restoring_history = False

    def enable_investment_accounting(
            self, reporting: CurrencyReporting, dt: datetime.datetime,
            opening_cash: dict[tuple[str, str, str], float],
    ) -> None:
        """Initialize native cash and FIFO accounting atomically on this ledger."""
        if self.native_currency_accounting:
            raise ValueError("Native currency accounting has already been initialized.")
        if self.position_tracker.get_position_list() and reporting.investment_history is None:
            raise ValueError("Opening positions require historical investment events; average cost is not enough.")
        candidate = Ledger(self.daily_returns_series.index, self._data_frequency, bond_book=self.bond_book)
        candidate._monetary_valuation = MonetaryValuationService(conversion=reporting.conversion, bond_book=self.bond_book)
        candidate.enable_currency_reporting(reporting.base_currency, dt, reporting.conversion)
        candidate.native_currency_accounting = True
        candidate._investment_base = reporting.base_currency
        for currency in dict.fromkeys((reporting.base_currency, *reporting.currencies)):
            candidate._ensure_currency(currency)
        history = reporting.investment_history
        if history is not None:
            candidate.restore_accounting_history(history)
            if any(event.dt > dt for event in (*history.events, *history.cash_events)):
                raise ValueError("Opening investment history contains events after the simulation start.")
            if any(event.dt > dt for _, event in history.income_due):
                raise ValueError("Opening income entitlements must be historical income events.")
        else:
            for key, amount in opening_cash.items():
                candidate.record_cash(CashEvent(id="opening", dt=dt, key=key, amount=amount, kind=CashEventKind.OPENING))
        marks = self._position_marks()
        self._reconcile_quantities(candidate.investment_quantities)
        snapshot = candidate.investment_snapshot(reporting.base_currency, dt, marks=marks)
        cash = candidate.cash_value(reporting.base_currency, dt)
        for name in ("_postings", "_posting_identities", "_accounting_keys", "_buy_lots_by_asset", "_fill_opening",
                     "_money_flows", "_currency_totals", "_cash_events", "_monetary_valuation"):
            setattr(self, name, getattr(candidate, name))
        if history is not None:
            self.position_tracker.restore_income(history.income_due, posted=history.events)
        self.native_currency_accounting = True
        self._opening_posting_count = len(self._postings)
        self._cash_flow_baselines = {}
        self._investment_base = reporting.base_currency
        self.enable_currency_reporting(reporting.base_currency, dt, reporting.conversion)
        self._portfolio.positions_value = self._portfolio.positions_exposure = snapshot.market_value
        self._portfolio.cash = cash
        self._portfolio.starting_cash = self._portfolio.portfolio_value = cash + snapshot.market_value
        self._portfolio.pnl = self._portfolio.returns = 0.0
        self._dirty_portfolio = self._dirty_account = True
        self._update_investment_portfolio()
        self.update_account()

    def _position_marks(self) -> dict[InvestmentKey, tuple[float, datetime.datetime]]:
        marks = {}
        for key, position in self.position_tracker.positions.items():
            if position.last_sale_date is None:
                raise ValueError(f"No timestamped market mark for {position.asset}.")
            marks[key] = (position.last_sale_price, position.last_sale_date)
        return marks

    def _reconcile_quantities(self, expected: Mapping[InvestmentKey, float]) -> None:
        actual = {key: position.amount for key, position in self.position_tracker.positions.items()}
        for key in expected.keys() | actual.keys():
            if not math.isclose(expected.get(key, 0.0), actual.get(key, 0.0), abs_tol=1e-9):
                raise ValueError(f"Investment lots do not reconcile to execution positions for {key}.")

    def investment_snapshot(
            self, currency: str, dt: datetime.datetime,
            marks: Mapping[InvestmentKey, tuple[float, datetime.datetime]] | None = None,
    ) -> InvestmentSnapshot:
        """Value the ledger's existing FIFO costs and current holdings in a requested currency."""
        if not self.native_currency_accounting:
            raise ValueError("Enable investment_pnl in CurrencyReporting before requesting investment PNL.")
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("Investment valuation timestamps must be timezone-aware.")
        if marks is None:
            self._reconcile_quantities(self.investment_quantities)
            marks = self._position_marks()
        if self._postings and self._postings[-1].dt > dt:
            # Historical queries use the same posting/FIFO implementation on the requested prefix.
            historical = Ledger(self.daily_returns_series.index, self._data_frequency, bond_book=self.bond_book)
            historical.native_currency_accounting = True
            historical._monetary_valuation = self._monetary_valuation
            historical._currency_conversion = self._currency_conversion
            historical.restore_accounting_history(InvestmentHistory(
                events=tuple(self._history_event(posting) for posting in self._postings if posting.dt <= dt),
                cash_events=tuple(event for event in self.cash_events if event.dt <= dt)))
            return historical.investment_snapshot(currency, dt, marks=marks)
        totals = self._ensure_currency(currency)
        positions = []
        for key in self._accounting_keys:
            lots = self._buy_lots_by_asset.get(key, ())
            quantity = math.fsum(lot.quantity for lot in lots)
            cost = math.fsum(lot.cost_by_currency[currency] for lot in lots)
            valuation = None
            if not math.isclose(quantity, 0.0, abs_tol=1e-12):
                if key not in marks:
                    raise ValueError(f"No market mark supplied for investment position {key}.")
                price, price_at = marks[key]
                valuation = self._monetary_valuation.value(
                    asset=key[2], quantity=quantity, price=price, price_at=price_at, dt=dt, currency=currency)
            value = valuation.value if valuation is not None else 0.0
            positions.append(InvestmentPosition(
                key=key, quantity=quantity, historical_cost=cost, market_value=value,
                realized_pnl=totals["realized"].get(key, 0.0), unrealized_pnl=value - cost,
                income=totals["income"].get(key, 0.0), fees=totals["fees"].get(key, 0.0), valuation=valuation))
        return InvestmentSnapshot(currency=currency, valuation_at=dt, positions=tuple(positions))

    def reporting_cash_flow(self, currency: str) -> float:
        """Simulation-period internal flows at each posting's own FX rate."""
        total = math.fsum(self._ensure_currency(currency)["cash_flow"].values())
        if currency not in self._cash_flow_baselines:
            self._cash_flow_baselines[currency] = math.fsum(
                amount * self._currency_conversion.rate(base=key[2], quote=currency, dt=posting.dt)
                for posting in self._postings[:self._opening_posting_count]
                for key, amount in self._posting_cash_changes(posting))
        return total - self._cash_flow_baselines[currency]

    def currency_snapshot(self, currency: str, dt: datetime.datetime):
        """Return read-only reporting data; performance baselines belong to MetricsTracker."""
        rate = self._currency_conversion.rate(base=self.reporting_base_currency, quote=currency, dt=dt)
        native = self.portfolio
        for position in self.position_tracker.get_position_list():
            self.validate_reporting_asset(position.asset)
        investment = self.investment_snapshot(currency, dt) if self.native_currency_accounting else None
        cash = self.cash_value(currency, dt) if investment is not None else native.cash * rate
        positions_value = investment.market_value if investment is not None else native.positions_value * rate
        portfolio = replace(
            native, cash=cash, portfolio_value=cash + positions_value,
            cash_flow=self.reporting_cash_flow(currency) if investment is not None else native.cash_flow * rate,
            positions_value=positions_value, positions_exposure=(
                positions_value if investment is not None else native.positions_exposure * rate),
            positions={key: replace(position) for key, position in native.positions.items()})
        account = copy(self.account)
        nonmonetary = {"cushion", "day_trades_remaining", "leverage", "net_leverage", "gross_leverage"}
        for field in fields(self.account):
            if field.name not in nonmonetary:
                setattr(account, field.name, getattr(self.account, field.name) * rate)
        stats = copy(self.position_tracker.stats)
        for name in ("gross_exposure", "gross_value", "long_exposure", "long_value",
                     "net_exposure", "net_value", "short_exposure", "short_value"):
            setattr(stats, name, getattr(stats, name) * rate)
        if stats.position_exposure_array is not None:
            stats.position_exposure_array = stats.position_exposure_array.copy() * rate
        if stats.position_exposure_series is not None:
            stats.position_exposure_series = stats.position_exposure_series.copy() * rate
        if investment is not None:
            stats.long_value = math.fsum(item.market_value for item in investment.positions if item.quantity > 0)
            stats.short_value = math.fsum(item.market_value for item in investment.positions if item.quantity < 0)
            stats.long_exposure, stats.short_exposure = stats.long_value, stats.short_value
            stats.net_value = stats.net_exposure = investment.market_value
            stats.gross_value = stats.gross_exposure = stats.long_value - stats.short_value
            values = {item.key: item.market_value for item in investment.positions}
            if stats.position_exposure_array is not None:
                stats.position_exposure_array[:] = [values[(position.exchange_name, position.trading_account_id,
                                                           position.asset)]
                                                    for position in self.position_tracker.get_position_list()]
        return portfolio, account, stats, investment

    def investment_history(self) -> InvestmentHistory:
        if not self.native_currency_accounting:
            raise ValueError("Enable investment_pnl before exporting investment history.")
        return InvestmentHistory(events=self.investment_events, cash_events=self.cash_events,
                                 income_due=self.position_tracker.income_due)

    @staticmethod
    def _cash_balance(amounts: Iterable[float]) -> float:
        """Sum native postings, treating only a few ULPs of roundoff as zero."""
        values = list(amounts)
        balance = math.fsum(values)
        rounding = 4 * math.ulp(max((abs(value) for value in values), default=0.0))
        return 0.0 if -rounding <= balance < 0 else balance

    @staticmethod
    def _posting_cash_changes(posting: Transaction | InvestmentEvent):
        if isinstance(posting, Transaction):
            yield (*posting.key[:2], posting.asset.quote.asset_name), -posting.amount * posting.native_unit_value
            if isinstance(posting.asset.asset, Currency):
                yield (*posting.key[:2], posting.asset.asset.asset_name), posting.amount
        elif posting.kind == InvestmentEventKind.FEE:
            yield (*posting.key[:2], posting.currency), -posting.amount
        elif posting.kind == InvestmentEventKind.INCOME:
            yield (*posting.key[:2], posting.currency), posting.amount

    def _cash_postings(self):
        for event in self._cash_events.values():
            yield event.dt, event.key, event.amount
        for posting in self._postings:
            for key, amount in self._posting_cash_changes(posting):
                yield posting.dt, key, amount

    def validate_cash_changes(self, dt: datetime.datetime, changes: Mapping[CashKey, float]) -> None:
        """Reject proposed native cash changes before mutating any postings.

        Balances are scoped by exchange, account, and currency. Future funding
        cannot pay an earlier debit. Backdated changes must also leave all
        already-posted later balances nonnegative.
        """
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("Cash changes need a timezone-aware timestamp.")
        if any(not math.isfinite(amount) for amount in changes.values()):
            raise ValueError("Cash changes must be finite.")
        if self._restoring_history:
            return
        amounts = {key: [change] for key, change in changes.items()}
        future: dict[datetime.datetime, dict[CashKey, list[float]]] = {}
        for stamp, key, amount in self._cash_postings():
            if key not in amounts:
                continue
            if stamp <= dt:
                amounts[key].append(amount)
            else:
                future.setdefault(stamp, {}).setdefault(key, []).append(amount)

        def check(stamp):
            for (exchange, account, currency), values in amounts.items():
                balance = self._cash_balance(values)
                if balance < 0 and changes[(exchange, account, currency)] < 0:
                    raise ValueError(
                        f"Insufficient {currency} cash for exchange {exchange}, account {account} "
                        f"at {stamp.isoformat()}: balance would be {balance:g}. "
                        "Fund this currency or perform an explicit currency exchange before spending it."
                    )

        check(dt)
        for stamp, postings in sorted(future.items()):
            for key, values in postings.items():
                amounts[key].extend(values)
            check(stamp)

    def record_cash(self, event: CashEvent) -> bool:
        """Record an opening balance, external funding, or executed FX cash leg.

        Parameters
        ----------
        event : CashEvent
            Signed native posting with an aware timestamp, finite amount, valid
            ISO currency, and nonempty identity/account scope.

        Returns
        -------
        bool
            ``True`` for a new posting, ``False`` for an identical scoped duplicate.

        Raises
        ------
        ValueError
            If the posting is invalid, overdraws its native cash balance, or
            conflicts with the same ``(key, id)``.
        """
        if event.dt.tzinfo is None or event.dt.utcoffset() is None or not math.isfinite(event.amount):
            raise ValueError("Cash events need an aware timestamp and finite amount.")
        if not event.id or not all(event.key):
            raise ValueError("Cash events need an identity, account scope, and currency.")
        ISOCurrency(value=event.key[2])
        identity = (event.key, event.id)
        existing = self._cash_events.get(identity)
        if existing is not None:
            if existing != event:
                raise ValueError(f"Conflicting cash event {event.id}.")
            return False
        if event.kind == CashEventKind.OPENING and event.amount < 0:
            raise ValueError("Opening native cash balances must be nonnegative.")
        self.validate_cash_changes(dt=event.dt, changes={event.key: event.amount})
        self._cash_events[identity] = event
        return True

    def cash_balances(self, dt: datetime.datetime) -> dict[CashKey, float]:
        """Reconstruct native balances through an inclusive aware timestamp.

        Parameters
        ----------
        dt : datetime.datetime
            Aware as-of time; later cash and investment events are excluded.

        Returns
        -------
        dict[CashKey, float]
            Balances by ``(exchange, account, currency)``, including derived trade
            consideration, fee debits, and paid income. Zero balances are
            retained and no implicit FX trade is performed.
        """
        amounts: dict[CashKey, list[float]] = {}
        for stamp, key, amount in self._cash_postings():
            if stamp <= dt:
                amounts.setdefault(key, []).append(amount)
        return {key: self._cash_balance(values) for key, values in amounts.items()}

    def cash_value(self, currency: str, dt: datetime.datetime) -> float:
        """Sum native cash after translating each balance at valuation-time FX.

        Parameters
        ----------
        currency : str
            Destination ISO currency code.
        dt : datetime.datetime
            Inclusive aware balance and FX valuation time.

        Returns
        -------
        float
            Net cash equity in ``currency``. Missing/stale FX for nonzero
            balances raises ``ValueError`` instead of dropping an account.
        """
        return sum(amount * self._monetary_valuation.conversion.rate(base=key[2], quote=currency, dt=dt)
                   for key, amount in self.cash_balances(dt=dt).items() if amount != 0)

    def external_value(self, currency: str, dt: datetime.datetime) -> float:
        """Sum external contributions at their own posting-time FX rates.

        Parameters
        ----------
        currency : str
            Destination ISO currency code.
        dt : datetime.datetime
            Inclusive aware cutoff for contributions and withdrawals.

        Returns
        -------
        float
            Net external capital to exclude from PNL. Opening balances and FX
            executions are excluded; deposits remain at historical contribution FX.
            Missing/stale event-time FX raises ``ValueError``.
        """
        return sum(event.amount * self._monetary_valuation.conversion.rate(base=event.key[2], quote=currency, dt=event.dt)
                   for event in self._cash_events.values() if event.dt <= dt and event.kind == CashEventKind.EXTERNAL)

    def exchange_cash(
            self, identity: str, dt: datetime.datetime, exchange: str, account: str,
            source_currency: str, currency: str, sold: float, received: float,
    ) -> bool:
        """Post both native legs of an actual FX execution, without submitting an order.

        Parameters
        ----------
        identity : str
            Nonempty execution ID shared by both cash legs for idempotent replay.
        dt : datetime.datetime
            Aware execution/posting time.
        exchange, account : str
            Nonempty execution exchange and trading-account identities.
        source_currency, currency : str
            Distinct ISO codes of the currency sold and the currency received.
        sold, received : float
            Positive finite executed native amounts, not an indicative FX rate.

        Returns
        -------
        bool
            ``True`` when both legs are new; ``False`` for an identical execution.

        Raises
        ------
        ValueError
            If inputs are invalid, source cash is insufficient, or an existing
            execution is conflicting/incomplete.

        Notes
        -----
        No valuation rate is used to infer consideration. This changes cash
        exposure, not security cost basis. The source cash must be available.
        """
        if source_currency == currency or not all(
                math.isfinite(value) and value > 0 for value in (sold, received)):
            raise ValueError("An FX execution needs distinct currencies and positive finite executed amounts.")
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("FX execution timestamps must be timezone-aware.")
        if not identity or not exchange or not account:
            raise ValueError("FX executions need an identity and explicit account scope.")
        ISOCurrency(value=source_currency)
        ISOCurrency(value=currency)
        events = (
            CashEvent(
                id=identity, dt=dt, key=(exchange, account, source_currency), amount=-sold, kind=CashEventKind.FX,
            ),
            CashEvent(id=identity, dt=dt, key=(exchange, account, currency), amount=received, kind=CashEventKind.FX),
        )
        existing = [self._cash_events.get((event.key, event.id)) for event in events]
        if any(item is not None for item in existing):
            if tuple(existing) == events:
                return False
            raise ValueError(f"Conflicting or incomplete FX execution {identity}.")
        self.validate_cash_changes(dt=dt, changes={event.key: event.amount for event in events})
        for event in events:
            self.record_cash(event=event)
        return True

    def _post_income(self, entitlements: list[InvestmentEvent]) -> None:
        """Post a validated income batch once; the tracker owns its unpaid entitlements."""
        payments = [replace(event, dt=self._reporting_dt or event.dt) for event in entitlements]
        pending: dict[CashKey, list[float]] = {}
        fresh = []
        for payment in payments:
            if self.native_currency_accounting and not self._validate_posting(posting=payment):
                continue
            # Validate every conversion and liability before changing cash or history.
            base = self.reporting_base_currency
            if base is not None and payment.currency != base:
                if self._currency_conversion is None:
                    raise ValueError("Foreign income requires a currency conversion service.")
                self._currency_conversion.rate(base=payment.currency, quote=base, dt=payment.dt)
            key = (*payment.key[:2], payment.currency)
            pending.setdefault(key, []).append(payment.amount)
            fresh.append(payment)
        if self.native_currency_accounting and fresh:
            self.validate_cash_changes(dt=fresh[0].dt, changes={
                key: math.fsum(values) for key, values in pending.items()})
        for payment in fresh:
            if self.native_currency_accounting:
                self.record_investment_event(event=payment, check_funding=False)
            self._cash_flow(amount=payment.amount, currency=payment.currency)

    def enable_currency_reporting(
            self, base_currency: str, dt: datetime.datetime, conversion: CurrencyConversionService | None = None,
    ) -> None:
        """Declare execution-book currency and set the initial aware reporting time.

        Parameters
        ----------
        base_currency : str
            ISO code used by execution-book monetary totals and reporting events.
        dt : datetime.datetime
            Aware initial time; a naive timestamp raises ``ValueError``.
        conversion : CurrencyConversionService or None, optional
            Shared FX service for foreign cash flows, including dividends in
            plain reporting mode. Investment accounting supplies this service.
        """
        self.reporting_base_currency = base_currency
        if conversion is not None:
            self._currency_conversion = conversion
        self.set_reporting_datetime(dt=dt)

    def set_reporting_datetime(self, dt: datetime.datetime) -> None:
        """Advance the event/valuation clock when currency reporting is enabled.

        Parameters
        ----------
        dt : datetime.datetime
            Aware posting time. Naive values raise ``ValueError`` in reporting mode;
            calls outside reporting mode do not change the reporting clock.
        """
        if self.reporting_base_currency is not None:
            if dt.tzinfo is None or dt.utcoffset() is None:
                raise ValueError("Currency reporting requires timezone-aware event timestamps.")
            self._reporting_dt = dt

    def validate_reporting_asset(self, asset: ExchangeAsset) -> None:
        """Reject instruments whose money cannot be accounted for in the configured mode.

        Parameters
        ----------
        asset : ExchangeAsset
            Listing about to enter the book.

        Raises
        ------
        ValueError
            In plain reporting, if quote or margin currency differs from the
            declared base; in investment mode, if instrument unit valuation is
            unsupported (including margined futures/options).

        Notes
        -----
        Without reporting this is a no-op. Investment mode allows mixed quote
        currencies but does not substitute principal accounting for margin settlement.
        """
        base = self.reporting_base_currency
        if self.native_currency_accounting:
            self._monetary_valuation.unit_value(asset=asset, price=0.0, dt=self._reporting_dt)
            return
        if base is not None and asset.quote.asset_name != base:
            raise ValueError(
                f"Currency reporting requires a single {base} book; {asset} is quoted in "
                f"{asset.quote.asset_name}. Mixed-native-currency accounting is not supported.")
        if base is not None and (
            isinstance(asset.asset, FuturesContract) or _is_margined_option(instrument=asset.asset)
        ):
            if margin_currency_of(asset=asset) != base:
                raise ValueError(f"{asset} settles margin outside the reporting book's base currency {base}.")

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
        self.set_reporting_datetime(dt=dt)
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

    def _cash_flow(self, amount: float, currency: str | None = None) -> None:
        """Apply an internal cash flow and retain its base-currency reporting event.

        Parameters
        ----------
        amount : float
            Signed cash movement: positive credits, negative debits.
        currency : str or None, optional
            Native ISO code; converts ``amount`` to the reporting base at the
            current reporting time. ``None`` means the
            amount is already base/native execution-book money.

        Notes
        -----
        Native investment cash is derived from the corresponding investment
        event; this updates execution aggregates/flow metrics, not a second
        native cash posting. External funding must use ``capital_change``.
        """
        if self.native_currency_accounting:
            self._dirty_portfolio = True
            try:
                rate = (self._currency_conversion.rate(base=currency, quote=self._investment_base, dt=self._reporting_dt)
                        if currency is not None else 1.0)
                self._portfolio.cash += amount * rate
                self._portfolio.cash_flow = self.reporting_cash_flow(self._investment_base)
            except ValueError:
                # Report valuation will raise until FX is available; executed facts stay recorded.
                pass
            return
        if (currency is not None and self.reporting_base_currency is not None
                and currency != self.reporting_base_currency):
            if self._currency_conversion is None:
                raise ValueError("Foreign cash flows require a currency conversion service.")
            amount *= self._currency_conversion.rate(
                base=currency, quote=self.reporting_base_currency, dt=self._reporting_dt)
        if self.reporting_base_currency is not None:
            if self._reporting_dt is None:
                raise ValueError("No timestamp set for a reporting cash flow.")
            self.reporting_events.append((self._reporting_dt, amount, False))
        self._dirty_portfolio = True
        self._portfolio.cash_flow += amount
        self._portfolio.cash += amount

    def cash_balance(self, currency: str, exchange_name: str, trading_account_id: str) -> float:
        """Current posted native cash for one venue/account/currency."""
        if not self.native_currency_accounting:
            raise ValueError("Native currency balances require investment accounting.")
        return self.cash_balances(dt=self._reporting_dt).get(
            (exchange_name, trading_account_id, currency), 0.0)

    def process_transaction(self, transaction: Transaction):
        """Add a transaction to ledger, updating the current state as needed.

        Parameters
        ----------
        transaction : Transaction
            The transaction to execute.

        Notes
        -----
        Investment mode first records a scoped native fill with instrument-aware
        monetary consideration; identical fill repeats do nothing. Historical
        cost uses fill-time FX. Commissions are posted separately through
        ``process_commission`` to avoid charging the fill's fee twice.
        Funding is checked before submission; already-executed fills are
        recorded without a funding veto if actual costs exceed the projection.
        """
        asset = transaction.asset
        self.validate_reporting_asset(asset=asset)
        if transaction.native_unit_value is None:
            if self.native_currency_accounting:
                transaction.native_unit_value = self._monetary_valuation.unit_value(
                    asset=asset, price=transaction.price, dt=transaction.dt)
            elif isinstance(asset.asset, Bond):
                transaction.native_unit_value = self.bond_price_in_money(asset, transaction.price, transaction.dt)
            elif isinstance(asset.asset, OptionContract):
                transaction.native_unit_value = transaction.price * asset.asset.multiplier
            else:
                transaction.native_unit_value = transaction.price
        if not self._record_posting(transaction):
            return
        self.set_reporting_datetime(dt=transaction.dt)
        # `asset` is the exchange listing; the instrument itself hangs off `.asset`.
        #
        # A margined option settles the way a future does -- no premium changes hands, variation
        # margin accrues daily -- so it joins the futures branch rather than getting one of its
        # own. That is the whole of what makes a MOEX book different from an OPRA one; see
        # `PremiumStyle`.
        if isinstance(asset.asset, FuturesContract) or _is_margined_option(asset.asset):
            if not self.futures_margin_model.models_margin:
                self.futures_margin_model.warn_once()
            try:
                old_price = self._payout_last_sale_prices[asset]
            except KeyError:
                self._payout_last_sale_prices[asset] = transaction.price
            else:
                positions = self.position_tracker.positions_by_asset.get((asset,))
                position = next(iter(positions), None) if positions else None
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
        elif self.native_currency_accounting:
            self._cash_flow(amount=-transaction.native_unit_value * transaction.amount,
                            currency=asset.quote.asset_name)
        elif isinstance(asset.asset, Bond):
            # A bond settles at its dirty price: the quote is a percentage of face value, and the
            # buyer additionally hands the seller the coupon accrued since the last payment. Both
            # corrections are needed -- the first is a factor of ten on a 1000 nominal, the second
            # averages half a coupon on every round trip.
            self._cash_flow(amount=-(self.bond_price_in_money(asset=asset, quoted_price=transaction.price,
                                                       dt=transaction.dt) * transaction.amount),
                            currency=asset.quote.asset_name)
        elif isinstance(asset.asset, OptionContract):
            # A premium-paid option. The premium is quoted per unit of the underlying and one
            # contract covers `multiplier` of them, so buying one 1.20 call costs 120, not 1.20 --
            # a hundredfold error in cash, and one that compounds: it also sets the cost basis
            # every later P&L is measured from.
            self._cash_flow(amount=-(transaction.price * asset.asset.multiplier * transaction.amount),
                            currency=asset.quote.asset_name)
        else:
            self._cash_flow(amount=-(transaction.price * transaction.amount), currency=asset.quote.asset_name)
        # print("LEVERAGE: BEFORE EXCEUTION", self.account.leverage, self.account.net_leverage)

        currency_trade = self.native_currency_accounting and isinstance(asset.asset, Currency)
        if currency_trade:
            self._cash_flow(amount=transaction.amount, currency=asset.asset.asset_name)
        else:
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

        if currency_trade:
            return

    def process_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.

        Parameters
        ----------
        splits: list[(Asset, float)]
            A list of splits. Each split is a tuple of (asset, ratio).

        Notes
        -----
        In investment mode, split records must leave whole units. Fractional
        cash-in-lieu requires an explicit disposal and raises ``ValueError``.
        Recorded splits preserve historical cost and identical repeats are ignored.
        """
        splits = list(splits)
        if self.native_currency_accounting:
            quantities = self.investment_quantities
            existing = {(event.key, event.id): event for event in self.investment_events}
            pending = []
            applicable = []
            for split in splits:
                events = []
                for key, position in self.position_tracker.positions.items():
                    if position.asset.asset.id == split.asset.id:
                        event = InvestmentEvent(
                            id=f"split-{split.asset.id}-{self._reporting_dt}", dt=self._reporting_dt,
                            kind=InvestmentEventKind.SPLIT, key=key,
                            currency=position.asset.quote.asset_name, amount=split.ratio)
                        prior = existing.get((key, event.id))
                        if prior is not None:
                            if prior != event:
                                raise ValueError(f"Conflicting investment split {event.id}.")
                            continue
                        if not math.isfinite(split.ratio) or split.ratio <= 0:
                            raise ValueError("Split ratio must be positive and finite.")
                        quantity = quantities[key] / split.ratio
                        if not math.isclose(a=quantity, b=round(quantity), abs_tol=1e-10):
                            raise ValueError("Fractional split cash-in-lieu requires an explicit disposal event.")
                        quantities[key] = quantity
                        existing[(key, event.id)] = event
                        events.append(event)
                if events:
                    pending.extend(events)
                    applicable.append(split)
            for event in pending:
                self.record_investment_event(event=event)
                self.position_tracker.positions[event.key].last_sale_price *= event.amount
            splits = applicable
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

    def process_commission(self, commission: Commission, tr):
        """Process the commission.

        Parameters
        ----------
        commission : Commission
            Native quote-currency fee with its transaction, stable fee ID, and
            optional aware posting time. The linked fill must already be recorded.

        tr
            Legacy caller context; retained for API compatibility and not used.

        Notes
        -----
        Investment mode charges a scoped fee once, converts at posting-time FX,
        and allocates it to the referenced fill's remaining basis/realized PNL.
        """
        if commission.asset != commission.transaction.asset:
            raise ValueError("Commission asset must match its transaction asset.")
        if self.native_currency_accounting and commission.amount:
            fill = self._posting_identities.get((commission.transaction.key, commission.transaction_id))
            if not isinstance(fill, Transaction):
                raise ValueError("Commission transaction must already be recorded in the ledger.")
            stamp = commission.dt if commission.dt is not None else self._reporting_dt
            if not self.record_execution(event=InvestmentEvent(
                    id=commission.id, dt=stamp, kind=InvestmentEventKind.FEE, key=fill.key,
                    currency=commission.asset.quote.asset_name,
                    amount=commission.amount, transaction_id=fill.id)):
                return
            self.set_reporting_datetime(dt=stamp)
            position = self.position_tracker.positions.get(fill.key)
            if position is not None:
                self.position_tracker.adjust_commission_cost_basis(position=position, cost=commission.amount)
                self.position_tracker._dirty_stats = True
        elif not self.native_currency_accounting:
            self.position_tracker.handle_commission(asset=commission.asset, cost=commission.amount)
        # print(f"Commission 2 for {asset.asset_name} is {cost}", tr.account.leverage, tr.account.net_leverage)
        self._cash_flow(amount=-commission.amount, currency=commission.asset.quote.asset_name)
        # print(f"Commission 3 for {asset.asset_name} is {cost}", tr.account.leverage, tr.account.net_leverage)

    def close_position(self, asset: ExchangeAsset, dt: datetime.datetime,
                       price: float | None = None):
        """Force-close a position whose contract has reached its auto close date.

        For a physically delivered contract this is the step that keeps the backtest honest: the
        position is being closed precisely so that it does not become a delivery obligation, and
        that is worth saying rather than letting it look like ordinary housekeeping.

        Args:
            price: What to close at, defaulting to the position's last mark. An expiring option is
                closed at its **settlement** value instead -- ``max(S - K, 0)`` for a call --
                because the last mark is a quote and the settlement is an arithmetic fact about
                where the underlying finished. The two part company exactly where it matters most:
                a 0DTE wing that stops being quoted an hour before the close carries a mark of a
                few cents into a settlement of zero, and the strike that finishes a dime in the
                money carries a mark of nothing into a payout of ten dollars a contract.
        """
        instrument = asset.asset
        if getattr(instrument, "is_deliverable", False):
            self.logger.warning(
                "Closing a deliverable futures position to avoid delivery",
                symbol=asset.symbol, notice_date=str(getattr(instrument, "notice_date", None)),
                expiration_date=str(getattr(instrument, "expiration_date", None)), dt=str(dt))

        for txn in self.position_tracker.close_positions(asset=asset, dt=dt, price=price):
            self.process_transaction(transaction=txn)

    def bond_price_in_money(self, asset: ExchangeAsset, quoted_price: float, dt) -> float:
        """What one bond of ``asset`` changes hands for at ``quoted_price``: clean value plus
        accrued coupon interest."""
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
                if self.native_currency_accounting and any(
                        event.event_type is BondEventType.AMORTIZATION for event in earned):
                    raise ValueError(
                        "Investment accounting for bond amortization requires principal-allocation events."
                    )
                self.position_tracker.earn_bond_payments(earned, earned_at=self._reporting_dt)

        self.position_tracker.pay_income(session=session_date, post=self._post_income)

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

        Earn dividends whose ex-date is this session and pay all previously
        earned dividends whose pay-date is on or before this session.
        """
        session_date = next_session.date() if isinstance(next_session, datetime.datetime) else next_session
        position_tracker = self.position_tracker

        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        held_assets = [pos.asset.asset for pos in position_tracker.get_position_list()]
        if held_assets:
            cash_dividends = await asset_service.get_cash_dividends_with_ex_date(
                assets=held_assets, date=session_date
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
                earned_at=self._reporting_dt,
            )

        position_tracker.pay_dividends(session_date, post=self._post_income)

    async def capital_change(
            self, change_amount: float, currency: str | None = None,
            exchange_name: str | None = None, trading_account_id: str | None = None,
            event_id: str | None = None,
    ):
        """Apply external capital without counting the transfer itself as PNL.

        Parameters
        ----------
        change_amount : float
            Signed finite deposit/withdrawal. Positive values add cash.
        currency : str or None, optional
            Native ISO code in investment mode; defaults to the investment
            accounting base. Currency-labelled changes require investment mode.
        exchange_name, trading_account_id : str or None, optional
            Explicit cash scope. Both may be omitted only when a single scope
            can be inferred from existing investment cash balances.
        event_id : str or None, optional
            Stable external-cash posting ID; generated when omitted. Identical
            scoped repeats are ignored, conflicting repeats raise.

        Raises
        ------
        ValueError
            If currency/account scope is invalid, duplicate data conflicts, or
            required price/FX observations are unavailable.

        Notes
        -----
        Revalues the book before applying the flow. Contributions are excluded
        from reporting PNL at the current posting-time FX rate, while subsequent
        native-cash FX changes remain portfolio gains/losses.
        """
        # `update_portfolio` is a coroutine. Called without awaiting, the portfolio value a target
        # capital change is measured against was whatever the last bar left behind, so the deposit
        # or withdrawal computed from it was wrong by a bar of profit and loss.
        await self.update_portfolio()
        if self.native_currency_accounting:
            scopes = {(key[0], key[1]) for key in self.cash_balances(dt=self._reporting_dt)}
            if exchange_name is not None and trading_account_id is not None:
                exchange, account = exchange_name, trading_account_id
            elif len(scopes) == 1 and exchange_name is None and trading_account_id is None:
                exchange, account = next(iter(scopes))
            else:
                raise ValueError("A capital change in a multi-account book requires an explicit scoped cash event.")
            native_currency = currency if currency is not None else self._investment_base
            rate = self._monetary_valuation.conversion.rate(
                base=native_currency, quote=self._investment_base, dt=self._reporting_dt)
            if not self.record_cash(
                event=CashEvent(
                    id=event_id if event_id is not None else uuid.uuid4().hex,
                    dt=self._reporting_dt,
                    key=(exchange, account, native_currency),
                    amount=change_amount,
                    kind=CashEventKind.EXTERNAL,
                ),
            ):
                return
            change_amount *= rate
        elif currency is not None:
            raise ValueError("Currency-labelled capital changes require investment accounting.")
        if self.reporting_base_currency is not None:
            if self._reporting_dt is None:
                raise ValueError("No timestamp set for a reporting capital change.")
            self.reporting_events.append((self._reporting_dt, change_amount, True))
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
        return [
            replace(position)
            for position in self.position_tracker.get_position_list()
        ]

    def futures_margin_by_currency(self, maintenance: bool = False) -> dict[str, float]:
        """Margin the open margined positions tie up, keyed by the currency it is posted in.

        Futures, and **margined options** -- a MOEX option ties up initial margin on both sides,
        because neither side has paid anything and either can end up owing. A premium-paid option
        is not counted: the buyer's risk is the premium they already handed over, and the writer's
        margin is a broker's rule rather than the exchange's, so the ledger does not invent one.

        Margin currency is a property of the exchange, not of the quote: an exchange may collect
        its local currency even for the contracts it quotes in dollars. Amounts in different currencies are reported
        separately because adding them would need an FX rate the ledger does not carry.
        """
        model = self.futures_margin_model
        totals: dict[str, float] = {}
        for position in self.position_tracker.get_position_list():
            instrument = position.asset.asset
            if not (isinstance(instrument, FuturesContract) or _is_margined_option(instrument)):
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

    def _get_payout_total(self, positions):

        total = 0
        for asset, old_price in self._payout_last_sale_prices.items():
            position = next(
                (position for position in positions.values() if position.asset == asset),
                None,
            )
            if position is None:
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

    @staticmethod
    def _positions_by_exchange_and_account(position_tracker: PositionTracker) -> dict:
        """Build the ``(exchange, trading account, asset) -> Position`` map the portfolio exposes.

        Flat, and keyed exactly as :class:`~ziplime.finance.domain.position_tracker.PositionTracker`
        keys its own positions, because that is what :class:`~ziplime.domain.portfolio.Portfolio`
        declares and what its accessors iterate. It used to build a nested
        ``exchange -> account -> asset -> Position`` dict instead, which left every one of those
        accessors reading a dict where it expected a position -- ``get_asset_positions_amount``
        raised ``AttributeError: 'dict' object has no attribute 'asset'`` the moment a strategy
        asked how much of something it held.

        The projected positions carry ``exchange_name`` and ``trading_account_id``: the accessors
        filter on both, and a projection without them silently matches nothing.
        """
        positions = {}
        for key, position in position_tracker.positions.items():
            positions[key] = Position(
                asset=position.asset,
                amount=position.amount,
                cost_basis=position.cost_basis,
                last_sale_price=position.last_sale_price,
                last_sale_date=position.last_sale_date,
                exchange_name=position.exchange_name,
                trading_account_id=position.trading_account_id,
            )
        return positions

    def synchronize_exchange_portfolio(self, portfolio: Portfolio):
        # start_cash = sum(exchange.get_start_cash_balance() for exchange in exchange_repository.get_all_exchanges())

        pt = self.position_tracker
        for (exchange_name, trading_account_id, asset), position in portfolio.positions.items():
            if position.amount == 0:
                continue
            pt.update_position(
                asset=asset, exchange_name=exchange_name,
                last_sale_price=position.last_sale_price,
                last_sale_date=position.last_sale_date,
                cost_basis=position.cost_basis,
                amount=position.amount,
                trading_account_id=trading_account_id,
            )
        self._portfolio.cash = portfolio.cash
        self._portfolio.starting_cash = portfolio.starting_cash
        self._portfolio.portfolio_value = portfolio.portfolio_value

        self._portfolio.positions = self._positions_by_exchange_and_account(pt)
        position_stats = pt.stats

        self._portfolio.positions_value = position_value = position_stats.net_value
        self._portfolio.positions_exposure = position_stats.net_exposure
        payout_total = self._get_payout_total(pt.positions)
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
        if self.native_currency_accounting:
            self._update_investment_portfolio()
            return
        if not self._dirty_portfolio:
            return

        pt = self.position_tracker

        self._portfolio.positions = self._positions_by_exchange_and_account(pt)
        position_stats = pt.stats

        self._portfolio.positions_value = position_value = position_stats.net_value
        self._portfolio.positions_exposure = position_stats.net_exposure
        payout_total = self._get_payout_total(pt.positions)
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

    def _update_investment_portfolio(self) -> None:
        """Revalue native cash and security snapshots in the accounting base.

        Uses the current reporting timestamp, even without new trades, so FX
        alone can change portfolio PNL/returns. Updates converted position
        exposures and marks account aggregates dirty; external capital is
        handled separately by ``capital_change``. Requires initialized
        investment accounting and valid timestamped marks/FX.
        """
        snapshot = self.investment_snapshot(currency=self._investment_base, dt=self._reporting_dt)
        stats = self.position_tracker.stats
        stats.long_value = sum(position.market_value for position in snapshot.positions if position.quantity > 0)
        stats.short_value = sum(position.market_value for position in snapshot.positions if position.quantity < 0)
        stats.long_exposure, stats.short_exposure = stats.long_value, stats.short_value
        stats.net_value = stats.net_exposure = snapshot.market_value
        stats.gross_value = stats.gross_exposure = stats.long_value - stats.short_value
        if stats.position_exposure_array is not None:
            values = {position.key: position.market_value for position in snapshot.positions}
            stats.position_exposure_array[:] = [
                values[(position.exchange_name, position.trading_account_id, position.asset)]
                for position in self.position_tracker.get_position_list()]
        self._portfolio.positions = self._positions_by_exchange_and_account(
            position_tracker=self.position_tracker,
        )
        self._portfolio.positions_value = self._portfolio.positions_exposure = snapshot.market_value
        self._portfolio.cash = self.cash_value(currency=self._investment_base, dt=self._reporting_dt)
        self._portfolio.cash_flow = self.reporting_cash_flow(self._investment_base)
        start_value = self._portfolio.portfolio_value
        end_value = self._portfolio.cash + snapshot.market_value
        pnl = end_value - start_value
        self._portfolio.portfolio_value = end_value
        self._portfolio.pnl += pnl
        if start_value:
            self._portfolio.returns = (1 + self._portfolio.returns) * (1 + pnl / start_value) - 1
        self._dirty_portfolio = False
        self._dirty_account = True

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
