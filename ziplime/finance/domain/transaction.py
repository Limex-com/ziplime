import datetime
from dataclasses import dataclass

from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclass
class Transaction:
    id: str
    amount: int
    dt: datetime.datetime
    price: float | None
    exchange_name: str
    trading_account_id: str

    order_id: str = None
    asset: ExchangeAsset = None
    commission: float | None = None
    average_entry_price: float | None = 0.0
    realized_pnl: float = 0.0
    realized_pnl_percentage: float = 0.0
    # Actual money per unit, including instrument conventions. Older restored
    # history can retain this consideration without knowing the original quote.
    native_unit_value: float | None = None

    @property
    def key(self) -> tuple[str, str, ExchangeAsset]:
        return self.exchange_name, self.trading_account_id, self.asset

    def total_price(self) -> float:
        if self.price is None:
            raise ValueError("The original quoted price is unavailable in this restored transaction.")
        return self.price * self.amount
