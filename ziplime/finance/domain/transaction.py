import datetime
from dataclasses import dataclass

from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclass
class Transaction:
    id: str
    amount: int
    dt: datetime.datetime
    price: float
    exchange_name: str
    trading_account_id: str


    order_id: str = None
    asset: ExchangeAsset = None
    commission: float | None = None
    average_entry_price: float | None = 0.0
    realized_pnl: float = 0.0

    def total_price(self) -> float:
        return self.price * self.amount
