from dataclasses import dataclass
import datetime

from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclass(frozen=True)
class FuturesContract(Asset):
    """A single futures contract.

    Attributes:
        settlement_type: Cash or physical delivery. A deliverable contract must be closed before
            ``notice_date``; see :class:`~ziplime.assets.domain.settlement_type.SettlementType`.
        margin_currency: Currency the exchange collects margin in, which is **not** always the
            currency the contract is quoted in: an exchange may quote a contract in dollars and
            still collect margin in its local currency.
    """

    root_exchange_asset: ExchangeAsset | None
    root_asset: Asset
    root_symbol: str
    notice_date: datetime.date
    expiration_date: datetime.date
    multiplier: float
    tick_size: float
    settlement_type: SettlementType = SettlementType.CASH
    margin_currency: str = "RUB"

    @property
    def is_deliverable(self) -> bool:
        """True when holding to expiry would create a delivery obligation."""
        return self.settlement_type.is_deliverable
