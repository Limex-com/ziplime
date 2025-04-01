from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import StringPK
from ziplime.db.base_model import BaseModel


class Exchange(BaseModel):
    __tablename__ = "exchanges"

    exchange: Mapped[StringPK]
    canonical_name: Mapped[str]
    country_code: Mapped[str]
