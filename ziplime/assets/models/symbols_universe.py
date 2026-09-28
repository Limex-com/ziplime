from sqlalchemy.orm import Mapped, relationship

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.symbols_universe_asset import SymbolsUniverseAssetModel
from ziplime.core.db.annotated_types import StringPK
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseModel(BaseModel):
    __tablename__ = "symbols_universe"
    assets: Mapped[list[SymbolsUniverseAssetModel]] = relationship("SymbolsUniverseAssetModel")
    name: Mapped[StringPK]
    symbol: Mapped[str]
    universe_type: Mapped[str]

    def get_assets(self, ) -> list[Asset]:
        return []
