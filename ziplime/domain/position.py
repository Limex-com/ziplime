import datetime
from dataclasses import dataclass

from ziplime.assets.entities.exchange_asset import ExchangeAsset


@dataclass
class Position:
    asset: ExchangeAsset
    amount: int
    cost_basis: float  # per share
    last_sale_price: float
    last_sale_date: datetime.datetime | None = None
    #: Which book this position sits in. `Portfolio`'s accessors filter on both, so a projection
    #: built without them matches nothing as soon as a caller names an exchange or an account --
    #: and, since the comparison short-circuits on `None`, does so only for the callers that ask.
    exchange_name: str | None = None
    trading_account_id: str | None = None
