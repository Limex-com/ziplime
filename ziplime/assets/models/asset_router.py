from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import IntegerPK
from ziplime.core.db.base_model import BaseModel


class AssetRouter(BaseModel):
    __tablename__ = "asset_router"

    sid: Mapped[IntegerPK]
    asset_type: Mapped[str]
