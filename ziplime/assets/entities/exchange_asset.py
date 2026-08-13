import datetime
from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset
from ziplime.assets.entities.exchange_info import ExchangeInfo


@dataclass(frozen=True)
class ExchangeAsset:
    sid: int | None  # if SID is None, then it is a new asset and we want it to have automatically assigned SID
    symbol: str
    start_date: datetime.date | None
    end_date: datetime.date | None
    first_traded: datetime.date | None
    auto_close_date: datetime.date | None
    external_id: str
    exchange: ExchangeInfo
    asset: Asset
    quote: Asset

    def __hash__(self):
        return hash(self.sid)

    def __str__(self):
        return f"{self.symbol}-{self.mic}-({self.sid})"

    @property
    def mic(self) -> str:
        return self.exchange.mic
