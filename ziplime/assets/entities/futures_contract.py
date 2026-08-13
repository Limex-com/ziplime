from dataclasses import dataclass
import datetime

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclass(frozen=True)
class FuturesContract(Asset):
    root_exchange_asset: ExchangeAsset | None
    root_asset: Asset
    notice_date: datetime.date
    expiration_date: datetime.date
    multiplier: float
    tick_size: float
