"""
Position Tracking
=================

    +-----------------+----------------------------------------------------+
    | key             | value                                              |
    +=================+====================================================+
    | asset           | the asset held in this position                    |
    +-----------------+----------------------------------------------------+
    | amount          | whole number of shares in the position             |
    +-----------------+----------------------------------------------------+
    | last_sale_price | price at last sale of the asset on the exchange    |
    +-----------------+----------------------------------------------------+
    | cost_basis      | the volume weighted average price paid per share   |
    +-----------------+----------------------------------------------------+

"""
import dataclasses
import datetime

from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclasses.dataclass
class Position:
    asset: ExchangeAsset
    exchange_name: str
    trading_account_id: str
    amount: int
    cost_basis: float
    last_sale_price: float
    last_sale_date: datetime.datetime | None

    def __hash__(self):
        # Identity hash: two positions that compare equal are still distinct
        # position records, so they must occupy separate index slots.
        return id(self)

    def __repr__(self):
        return f"asset: {self.asset}, amount: {self.amount}, cost_basis: {self.cost_basis}," \
               f"last_sale_price: {self.last_sale_price}"

