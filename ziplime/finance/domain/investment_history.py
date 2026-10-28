import datetime
from dataclasses import dataclass

from ziplime.finance.domain.cash_event import CashEvent
from ziplime.finance.domain.investment_event import InvestmentEvent


@dataclass(frozen=True)
class InvestmentHistory:
    """Immutable replay inputs; fields are constructor arguments.

    Attributes
    ----------
    events : tuple[InvestmentEvent, ...]
        Chronologically posted native trades, fees, paid income, and splits.
    cash_events : tuple[CashEvent, ...]
        Explicit opening balances, external funding, and executed FX cash legs.
    income_due : tuple[tuple[datetime.date, InvestmentEvent], ...], optional
        Unpaid ``(payment_date, entitlement)`` pairs. Entitlements retain the
        account quantity at earning time; the position tracker owns these
        entitlements and the ledger sets posting time on payment.

    Notes
    -----
    This is a domain-event export, not a file/database serialization format.
    Callers must resolve listing identities and restore matching execution
    positions and timestamped marks separately. ``Ledger`` restores postings
    and the position tracker's unpaid entitlements. Trade records are exported
    from the ledger's original transactions; they are not stored a second time.
    """

    events: tuple[InvestmentEvent, ...]
    cash_events: tuple[CashEvent, ...]
    income_due: tuple[tuple[datetime.date, InvestmentEvent], ...] = ()
