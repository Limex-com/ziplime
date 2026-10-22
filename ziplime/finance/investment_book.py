import datetime
import math
from collections.abc import Iterable, Mapping

from iso4217 import Currency

from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.domain.cash_event import CashEvent, CashKey
from ziplime.finance.domain.cash_event_kind import CashEventKind
from ziplime.finance.domain.investment_history import InvestmentHistory
from ziplime.finance.domain.investment_position import InvestmentPosition
from ziplime.finance.domain.investment_snapshot import InvestmentSnapshot
from ziplime.finance.investment_projection import InvestmentProjection
from ziplime.finance.monetary_valuation_service import MonetaryValuationService


class InvestmentBook:
    """Native event history, independently replayable in any supported reporting currency.

    Parameters
    ----------
    valuation : MonetaryValuationService
        Instrument-aware valuation and historical/current as-of FX conversion.

    Attributes
    ----------
    valuation : MonetaryValuationService
        Shared service used to validate instruments and value holdings/cash.
    _events : list[InvestmentEvent]
        Chronological native investment postings.
    _identities : dict[tuple[InvestmentKey, str], InvestmentEvent]
        Scoped deduplication index for investment postings.
    _quantities : dict[InvestmentKey, float]
        Current signed quantities after all recorded trades/splits.
    _cash_events : dict[tuple[CashKey, str], CashEvent]
        Insertion-ordered explicit cash postings keyed by scoped identity.

    Notes
    -----
    Position keys are ``(exchange, account, listing)``; cash keys are
    ``(exchange, account, currency)``. Events retain native amounts and do not
    lock the book to one reporting currency. Snapshots replay FIFO history;
    they are not incrementally cached. Cash can be negative: this book does
    not enforce settlement, funding, margin, or borrowing constraints.
    """

    def __init__(self, valuation: MonetaryValuationService):
        """Create empty native investment and explicit cash-event histories."""
        self.valuation = valuation
        self._events: list[InvestmentEvent] = []
        self._identities: dict[tuple[InvestmentKey, str], InvestmentEvent] = {}
        self._quantities: dict[InvestmentKey, float] = {}
        self._cash_events: dict[tuple[CashKey, str], CashEvent] = {}

    @property
    def events(self) -> tuple[InvestmentEvent, ...]:
        """Return an immutable chronological view of posted investment events."""
        return tuple(self._events)

    @property
    def quantities(self) -> dict[InvestmentKey, float]:
        """Return a copy of current signed quantities by full position key, including zeros."""
        return dict(self._quantities)

    @property
    def cash_events(self) -> tuple[CashEvent, ...]:
        """Return explicit cash postings in insertion order, not derived trade/fee/income flows."""
        return tuple(self._cash_events.values())

    def history(self) -> InvestmentHistory:
        """Export immutable posted investment/cash events without execution positions.

        Returns
        -------
        InvestmentHistory
            Replay inputs for caller-managed storage. Unpaid income entitlements
            belong to the ledger; use ``Ledger.investment_history`` to include them.
        """
        return InvestmentHistory(events=self.events, cash_events=self.cash_events)

    @classmethod
    def from_history(cls, valuation: MonetaryValuationService, history: InvestmentHistory) -> "InvestmentBook":
        """Construct a book by replaying previously exported native postings.

        Parameters
        ----------
        valuation : MonetaryValuationService
            Instrument and FX service for the restored book; historical FX is
            required when subsequently projecting costs/PNL in another currency.
        history : InvestmentHistory
            Posted events with resolved ``ExchangeAsset`` identities. Unpaid
            entitlements are not paid or retained here; the ledger restores them.

        Returns
        -------
        InvestmentBook
            New book with the original native events, cash postings, and quantities.

        Raises
        ------
        ValueError
            If any posting fails ``record`` or ``record_cash`` validation.
        """
        book = cls(valuation=valuation)
        book.restore(events=history.events)
        for event in history.cash_events:
            book.record_cash(event=event)
        return book

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
            If the posting is invalid or conflicts with the same ``(key, id)``.
        """
        if event.dt.tzinfo is None or event.dt.utcoffset() is None or not math.isfinite(event.amount):
            raise ValueError("Cash events need an aware timestamp and finite amount.")
        if not event.id or not all(event.key):
            raise ValueError("Cash events need an identity, account scope, and currency.")
        Currency(value=event.key[2])
        identity = (event.key, event.id)
        existing = self._cash_events.get(identity)
        if existing is not None:
            if existing != event:
                raise ValueError(f"Conflicting cash event {event.id}.")
            return False
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
            consideration, fee debits, and paid income. Zero/negative balances are
            retained and no implicit FX trade is performed.
        """
        balances: dict[CashKey, float] = {}
        for event in self._cash_events.values():
            if event.dt <= dt:
                balances[event.key] = balances.get(event.key, 0.0) + event.amount
        for event in self._events:
            if event.dt > dt:
                break
            key = (event.key[0], event.key[1], event.currency)
            if event.kind == "trade":
                amount = -event.quantity * event.unit_value
            elif event.kind == "fee":
                amount = -event.amount
            elif event.kind == "income":
                amount = event.amount
            else:
                continue
            balances[key] = balances.get(key, 0.0) + amount
        return balances

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
        return sum(amount * self.valuation.conversion.rate(base=key[2], quote=currency, dt=dt)
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
        return sum(event.amount * self.valuation.conversion.rate(base=event.key[2], quote=currency, dt=event.dt)
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
            If inputs are invalid or an existing execution is conflicting/incomplete.

        Notes
        -----
        No valuation rate is used to infer consideration. This changes cash
        exposure, not security cost basis, and does not enforce available funding.
        """
        if source_currency == currency or not all(
                math.isfinite(value) and value > 0 for value in (sold, received)):
            raise ValueError("An FX execution needs distinct currencies and positive finite executed amounts.")
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("FX execution timestamps must be timezone-aware.")
        if not identity or not exchange or not account:
            raise ValueError("FX executions need an identity and explicit account scope.")
        Currency(value=source_currency)
        Currency(value=currency)
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
        for event in events:
            self.record_cash(event=event)
        return True

    def record(self, event: InvestmentEvent) -> bool:
        """Validate and append one native investment posting.

        Parameters
        ----------
        event : InvestmentEvent
            Chronological, aware trade/fee/income/split posting. A fee references
            an existing trade in the same position key. Trades use quote currency;
            splits use old-shares/new-shares ratios and must leave whole units.

        Returns
        -------
        bool
            ``True`` for a new posting, ``False`` for an identical ``(key, id)``.

        Raises
        ------
        ValueError
            If identity/scope, amounts, currency, instrument, chronology, fee
            association, or split quantity is invalid, or a duplicate conflicts.

        Notes
        -----
        Stores native consideration without requiring a particular reporting
        pair. Historical FX availability is checked when projecting a snapshot.
        """
        identity = (event.key, event.id)
        existing = self._identities.get(identity)
        if existing is not None:
            if event != existing:
                raise ValueError(f"Conflicting investment event {event.id}.")
            return False
        if event.dt.tzinfo is None or event.dt.utcoffset() is None:
            raise ValueError("Investment event timestamps must be timezone-aware.")
        if self._events and event.dt < self._events[-1].dt:
            raise ValueError("Investment events must be recorded in chronological posting order.")
        if not all(math.isfinite(value) for value in (event.quantity, event.unit_value, event.amount)):
            raise ValueError("Investment monetary values and quantities must be finite.")
        if not event.id or not event.key[0] or not event.key[1]:
            raise ValueError("Investment events need an identity and explicit account scope.")
        Currency(value=event.currency)
        quantity = self._quantities.get(event.key, 0.0)
        if event.kind == "trade":
            if event.quantity == 0 or event.unit_value < 0:
                raise ValueError("An investment fill needs a nonzero quantity and nonnegative unit value.")
            self.valuation.unit_value(asset=event.key[2], price=0.0, dt=event.dt)
            if event.currency != event.key[2].quote.asset_name:
                raise ValueError("Fill consideration must be denominated in the listing's quote currency.")
            quantity += event.quantity
        elif event.kind == "split":
            if event.amount <= 0:
                raise ValueError("Split ratio must be positive.")
            quantity /= event.amount
            if not math.isclose(a=quantity, b=round(quantity), abs_tol=1e-10):
                raise ValueError("Fractional split cash-in-lieu requires an explicit disposal event.")
        elif event.kind == "fee":
            if event.transaction_id is None:
                raise ValueError("Investment fees require a transaction_id.")
            fill = self._identities.get((event.key, event.transaction_id))
            if fill is None or fill.kind != "trade":
                raise ValueError("An investment fee must reference an existing fill in the same book.")
        elif event.kind != "income":
            raise ValueError(f"Unsupported investment event kind: {event.kind}.")
        self._events.append(event)
        self._identities[identity] = event
        self._quantities[event.key] = quantity
        return True

    def restore(self, events: Iterable[InvestmentEvent]) -> None:
        """Replay native investment events into an empty investment history.

        Parameters
        ----------
        events : Iterable[InvestmentEvent]
            Chronological postings with listing identities already resolved
            through the asset repository. Cash postings must be restored separately.

        Raises
        ------
        ValueError
            If investment events already exist or an input fails ``record``.
            Replay is sequential; valid earlier postings remain if a later one fails.
        """
        if self._events:
            raise ValueError("Restore requires an empty investment book.")
        for event in events:
            self.record(event=event)

    def snapshot(
            self, currency: str, dt: datetime.datetime,
            marks: Mapping[InvestmentKey, tuple[float, datetime.datetime]],
    ) -> InvestmentSnapshot:
        """Replay historical FIFO costs and value remaining holdings as of ``dt``.

        Parameters
        ----------
        currency : str
            Reporting ISO currency code for costs, values, income, fees, and PNL.
        dt : datetime.datetime
            Aware inclusive event cutoff and current valuation time.
        marks : Mapping[InvestmentKey, tuple[float, datetime.datetime]]
            Native ``(quoted_price, aware_availability_time)`` for each open
            position. Marks must not be future-dated or stale; closed positions
            require no mark. Extra supplied keys are ignored.

        Returns
        -------
        InvestmentSnapshot
            Immutable open/closed position accounting in first-event order.
            Acquisition costs, sale proceeds, fees, and income use event-time FX;
            open market values use valuation-time FX. Cash FX is excluded.

        Raises
        ------
        ValueError
            If time, marks, instrument values, or required historical/current
            FX observations are invalid or unavailable.
        """
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("Investment valuation timestamps must be timezone-aware.")
        projection = InvestmentProjection(currency=currency, conversion=self.valuation.conversion)
        for event in self._events:
            if event.dt > dt:
                break
            projection.apply(event=event)
        positions = []
        for key, lots in projection.lots.items():
            quantity = sum(lot.quantity for lot in lots)
            cost = sum(lot.cost for lot in lots)
            valuation = None
            if not math.isclose(a=quantity, b=0.0, abs_tol=1e-12):
                if key not in marks:
                    raise ValueError(f"No market mark supplied for investment position {key}.")
                price, price_at = marks[key]
                valuation = self.valuation.value(
                    asset=key[2],
                    quantity=quantity,
                    price=price,
                    price_at=price_at,
                    dt=dt,
                    currency=currency,
                )
            value = valuation.value if valuation is not None else 0.0
            positions.append(
                InvestmentPosition(
                    key=key,
                    quantity=quantity,
                    historical_cost=cost,
                    market_value=value,
                    realized_pnl=projection.realized.get(key, 0.0),
                    unrealized_pnl=value - cost,
                    income=projection.income.get(key, 0.0),
                    fees=projection.fees.get(key, 0.0),
                    valuation=valuation,
                ),
            )
        return InvestmentSnapshot(currency=currency, valuation_at=dt, positions=tuple(positions))
