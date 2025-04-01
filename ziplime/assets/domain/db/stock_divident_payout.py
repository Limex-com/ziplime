import datetime

from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import StringIndexed, DateIndexed, IntegerIndexed, StringPK
from ziplime.db.base_model import BaseModel


class StockDividendPayout(BaseModel):
    __tablename__ = "stock_dividend_payouts"

    index: Mapped[StringPK]
    sid: Mapped[IntegerIndexed]
    ex_date: Mapped[DateIndexed]

    declared_date: Mapped[datetime.date]
    record_date: Mapped[datetime.date]
    pay_date: Mapped[datetime.date]
    payment_sid: Mapped[str]
    ration: Mapped[float]
