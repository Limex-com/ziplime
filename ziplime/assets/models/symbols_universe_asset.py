import datetime

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import AssetRouterFKPK, Decimal8, SymbolsUniverseFKPK, \
    SymbolUniverseAssetStartDatePK
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseAssetModel(BaseModel):
    __tablename__ = "symbols_universe_assets"
    symbol_universe_name: Mapped[SymbolsUniverseFKPK]
    start_date: Mapped[SymbolUniverseAssetStartDatePK]
    end_date: Mapped[datetime.date | None]
    asset_id: Mapped[AssetRouterFKPK]
    # asset_router = relationship(AssetRouter)
    ratio: Mapped[Decimal8 | None]
