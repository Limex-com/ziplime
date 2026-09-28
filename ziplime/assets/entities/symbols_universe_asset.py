import datetime
from dataclasses import dataclass
from decimal import Decimal


from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class SymbolsUniverseAsset:
    symbol_universe_name: str
    start_date: datetime.date
    end_date: datetime.date | None
    asset: Asset
    ratio: Decimal | None
