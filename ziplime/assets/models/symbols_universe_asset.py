import datetime
from abc import abstractmethod

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import AssetRouterFKPK, Decimal8, SymbolsUniverseFKPK
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseAssetModel(BaseModel):
    __tablename__ = "symbols_universe_assets"
    symbol_universe_id: Mapped[SymbolsUniverseFKPK]
    asset_sid: Mapped[AssetRouterFKPK]
    # asset_router = relationship(AssetRouter)
    ratio: Mapped[Decimal8 | None]
