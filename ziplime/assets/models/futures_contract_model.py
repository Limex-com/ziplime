import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel
from ziplime.core.db.annotated_types import ExchangeAssetFK, AssetRouterFKPK


class FuturesContractModel(AssetModel):
    __tablename__ = "futures_contracts"
    root_asset_id: Mapped[AssetRouterFKPK]
    root_exchange_asset_sid: Mapped[ExchangeAssetFK | None]
    notice_date: Mapped[datetime.date]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
