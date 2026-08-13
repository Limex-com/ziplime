from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import DateIndexed, IntegerIndexed, StringPK, IntegerPK, AssetRouterFK
from ziplime.core.db.base_model import BaseModel


class SplitModel(BaseModel):
    __tablename__ = "splits"

    id: Mapped[IntegerPK]
    asset_id: Mapped[AssetRouterFK]

    effective_date: Mapped[DateIndexed]
    ratio: Mapped[float]
