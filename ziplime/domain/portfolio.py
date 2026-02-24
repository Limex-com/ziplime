import datetime
from dataclasses import dataclass, field

from ziplime.assets.entities.asset import Asset
from ziplime.domain.position import Position


@dataclass
class Portfolio:
    # capital_used: float
    cash_flow: float
    starting_cash: float
    portfolio_value: float
    pnl: float
    returns: float
    cash: float
    positions_value: float
    positions_exposure: float
    positions: dict[Asset, Position] = field(default_factory=dict)
    start_date: datetime.datetime | None = None


