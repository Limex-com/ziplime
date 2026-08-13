import datetime

from sqlalchemy.orm import Mapped, relationship, declared_attr

from ziplime.core.db.annotated_types import ExchangeFK, IntegerPK, AssetRouterFK
from ziplime.core.db.base_model import BaseModel


class ExchangeAssetModel(BaseModel):
    __tablename__ = "exchange_assets"
    sid: Mapped[IntegerPK]
    asset_id: Mapped[AssetRouterFK]
    quote_id: Mapped[AssetRouterFK]

    mic: Mapped[ExchangeFK]
    symbol: Mapped[str | None]

    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]

    first_traded: Mapped[datetime.date]
    auto_close_date: Mapped[datetime.date]

    external_id: Mapped[str]

    @declared_attr  # type: ignore [misc]
    def asset_router(cls):
        return relationship("AssetRouter", foreign_keys=f"{cls.__name__}.asset_id")

    def __hash__(self):
        return self.sid
