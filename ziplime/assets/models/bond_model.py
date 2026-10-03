import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel


class BondModel(AssetModel):
    __tablename__ = "bonds"

    face_value: Mapped[float]
    maturity_date: Mapped[datetime.date]
    coupon_rate: Mapped[float]
    coupon_frequency: Mapped[int]
    quote_currency: Mapped[str]
    day_count: Mapped[str]
    price_quotation: Mapped[str]
    is_amortized: Mapped[bool]
