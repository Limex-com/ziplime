from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import StringPK
from ziplime.core.db.base_model import BaseModel


class ExchangeInfo(BaseModel):
    __tablename__ = "exchanges"

    exchange: Mapped[StringPK]
    canonical_name: Mapped[str]
    country_code: Mapped[str]
