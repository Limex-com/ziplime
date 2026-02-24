import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel
from ziplime.core.db.annotated_types import StringUnique, FuturesRootSymbolFK, ExchangeFK


class FuturesContractModel(AssetModel):
    __tablename__ = "futures_contracts"

    symbol: Mapped[StringUnique]
    root_symbol: Mapped[FuturesRootSymbolFK]
    notice_date: Mapped[datetime.date]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
    exchange: Mapped[ExchangeFK]
