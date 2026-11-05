import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel
from ziplime.core.db.annotated_types import ExchangeAssetFK, AssetRouterFKPK, StringIndexed


class FuturesContractModel(AssetModel):
    __tablename__ = "futures_contracts"
    root_asset_id: Mapped[AssetRouterFKPK]
    #: The chain's root. Not a foreign key: a chain is keyed by ``(root_symbol, mic)`` and a
    #: contract learns its exchange from its listing, so the pair is not stored here.
    root_symbol: Mapped[StringIndexed]
    root_exchange_asset_sid: Mapped[ExchangeAssetFK | None]
    notice_date: Mapped[datetime.date]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
    settlement_type: Mapped[str]
    margin_currency: Mapped[str]
