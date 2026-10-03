import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class Asset:
    id: int | None  # if SID is None, then it is a new asset and we want it to have automatically assigned SID
    isin: str | None
    asset_name: str
    start_date: datetime.date | None
    end_date: datetime.date | None
    first_traded: datetime.date | None
    auto_close_date: datetime.date | None
    # mic: str | None

    # @abstractmethod
    # def get_symbol_by_exchange(self, exchange_name: str | None) -> str | None: ...

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return f"{self.asset_name}-{self.isin}-({self.id})"
