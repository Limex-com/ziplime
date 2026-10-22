from dataclasses import dataclass, field
import datetime
import uuid

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.domain.order import Order


@dataclass
class Commission:
    """Native fill fee posting; fields are constructor arguments.

    Attributes
    ----------
    asset : ExchangeAsset
        Listing whose quote currency denominates ``amount``.
    order : Order or None
        Associated order, when available; also disambiguates exchange/account
        scope when fill IDs are reused across accounts.
    amount : float
        Native fee charge; positive values debit cash, negative values are rebates.
    transaction_id : str or None, optional
        Fill ID to which the fee belongs. Investment accounting requires an
        unambiguous fill: omission is only resolved from a single matching order
        fill in the ledger's current transaction records.
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
    transaction_id: str | None = None
    dt: datetime.datetime | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
