import datetime

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import DateIndexed, IntegerIndexed, StringPK, IntegerPK, AssetRouterFK
from ziplime.core.db.base_model import BaseModel


class StockDividendPayoutModel(BaseModel):
    __tablename__ = "stock_dividend_payouts"

    id: Mapped[IntegerPK]
    asset_id: Mapped[AssetRouterFK]
    ex_date: Mapped[DateIndexed]

    declared_date: Mapped[datetime.date]
    record_date: Mapped[datetime.date]
    pay_date: Mapped[datetime.date]
    payment_sid: Mapped[str]
    ration: Mapped[float]
