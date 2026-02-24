import datetime

from ziplime.assets.entities.asset import Asset
from ziplime.protocol import DataSourceType


class Transaction:

    def __init__(self, id: str,
                 asset: Asset, amount: int, dt: datetime.datetime, price: float, order_id: str,
                 exchange_name: str,
                 commission: float | None = None):
        self.id = id
        self.asset = asset
        self.amount = amount
        self.exchange_name=exchange_name
        # if amount < 1:
        #     raise Exception("Transaction magnitude must be at least 1.")

        self.dt = dt
        self.price = price
        self.order_id = order_id
        self.type = DataSourceType.TRANSACTION
        self.commission = commission
        self.realized_pnl = 0.0

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "amount": self.amount,
            "dt": self.dt,
            "price": self.price,
            "order_id": self.order_id,
            "commission": self.commission,
            "asset": self.asset,
            "realized_pnl": self.realized_pnl
        }


    def total_price(self) -> float:
        return self.price * self.amount