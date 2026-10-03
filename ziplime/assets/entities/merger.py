import datetime
from dataclasses import dataclass

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class Merger:
    id: int
    asset: Asset
    effective_date: datetime.date
    ratio: float
