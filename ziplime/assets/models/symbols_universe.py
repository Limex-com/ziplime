import datetime
from abc import abstractmethod

from sqlalchemy.orm import Mapped, relationship, declared_attr

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.symbols_universe_asset import SymbolsUniverseAssetModel
from ziplime.core.db.annotated_types import AssetRouterFKPK, IntegerPK
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseModel(BaseModel):
    __tablename__ = "symbols_universe"
    id: Mapped[IntegerPK]
    assets: Mapped[list[SymbolsUniverseAssetModel]] = relationship("SymbolsUniverseAssetModel")
    name: Mapped[str]
    symbol: Mapped[str]
    universe_type: Mapped[str]

    def get_assets(self, ) -> list[Asset]:
        return []
