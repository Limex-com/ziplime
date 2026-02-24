from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import DateIndexed, IntegerIndexed, StringPK
from ziplime.core.db.base_model import BaseModel


class Dividend(BaseModel):
    __tablename__ = "dividends"

    index: Mapped[StringPK]
    sid: Mapped[IntegerIndexed]

    effective_date: Mapped[DateIndexed]
    ratio: Mapped[float]
