from dataclasses import dataclass, field
import datetime
import uuid

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.transaction import Transaction


@dataclass
class Commission:
    """Native fill fee posting; fields are constructor arguments.

    Attributes
    ----------
    asset : ExchangeAsset
        Listing whose quote currency denominates ``amount``.
    order : Order or None
        Associated order, when available, retained for execution reporting.
    amount : float
        Native fee charge; positive values debit cash, negative values are rebates.
    transaction : Transaction
        Executed fill to which the fee belongs, including exchange/account scope.
    transaction_id : str
        Read-only property returning ``transaction.id``; not a constructor argument.
    dt : datetime.datetime or None, optional
        Aware posting time used for fee FX. ``None`` uses the ledger's current
        reporting time; a late fee should carry its actual posting timestamp.
    id : str, optional
        Stable fee-event identity for idempotent investment accounting; generated
        as a UUID by default. It is distinct from the referenced trade ID.
    """

    asset: ExchangeAsset
    order: Order | None
    amount: float
    transaction: Transaction
    dt: datetime.datetime | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)

    @property
    def transaction_id(self) -> str:
        return self.transaction.id
