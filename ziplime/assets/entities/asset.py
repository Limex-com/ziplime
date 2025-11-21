import datetime
from abc import abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Asset:
    sid: int | None  # if SID is None, then it is a new asset and we want it to have automatically assigned SID
    asset_name: str
    start_date: datetime.date | None
    end_date: datetime.date | None
    first_traded: datetime.date | None
    auto_close_date: datetime.date | None
    # mic: str | None

    @abstractmethod
    def get_symbol_by_exchange(self, exchange_name: str | None) -> str | None: ...
