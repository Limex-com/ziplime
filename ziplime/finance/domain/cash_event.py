import datetime
from dataclasses import dataclass

from ziplime.finance.domain.cash_event_kind import CashEventKind

type CashKey = tuple[str, str, str]


@dataclass(frozen=True)
class CashEvent:
    """Immutable native-currency cash posting; fields are constructor arguments.

    Attributes
    ----------
    id : str
        Posting identity, unique within ``key``; identical repeats are idempotent.
    dt : datetime.datetime
        Aware posting timestamp.
    key : CashKey
        ``(exchange_name, trading_account_id, ISO_currency_code)`` cash identity.
    amount : float
        Signed native amount: positive credits cash, negative debits cash.
    kind : CashEventKind
        Opening balance, external contribution/withdrawal excluded from PNL,
        or one leg of an actual FX execution. Trades, income, and fees are
        derived from investment events instead of duplicated as cash events.
    """

    id: str
    dt: datetime.datetime
    key: CashKey
    amount: float
    kind: CashEventKind

    def __post_init__(self) -> None:
        """Normalize serialized strings to enum members and reject unsupported kinds."""
        object.__setattr__(self, "kind", CashEventKind(self.kind))
