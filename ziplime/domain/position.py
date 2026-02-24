import datetime
from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset


@dataclass
class Position:
    asset: Asset
    amount: int
    cost_basis: float  # per share
    last_sale_price: float
    last_sale_date: datetime.datetime | None = None
