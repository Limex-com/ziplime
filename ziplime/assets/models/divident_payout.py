import datetime

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import DateIndexed, IntegerIndexed, StringPK
from ziplime.core.db.base_model import BaseModel


class DividendPayout(BaseModel):
    __tablename__ = "dividend_payouts"
    index: Mapped[StringPK]
    sid: Mapped[IntegerIndexed]
    ex_date: Mapped[DateIndexed]
    declared_date: Mapped[datetime.date]
    record_date: Mapped[datetime.date]
    pay_date: Mapped[datetime.date]
    amount: Mapped[int]
