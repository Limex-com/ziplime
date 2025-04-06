import datetime

from sqlalchemy.orm import Mapped, relationship, declared_attr

from ziplime.db.annotated_types import AssetRouterFKPK
from ziplime.db.base_model import BaseModel


class Asset(BaseModel):
    __tablename__ = "assets"
    __abstract__ = True

    sid: Mapped[AssetRouterFKPK]
    # asset_router = relationship(AssetRouter)
    asset_name: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    first_traded: Mapped[datetime.date]
    auto_close_date: Mapped[datetime.date]

    @declared_attr  # type: ignore [misc]
    def asset_router(cls):
        return relationship("AssetRouter", foreign_keys=f"{cls.__name__}.sid")

    def __hash__(self):
        return self.sid
