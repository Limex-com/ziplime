from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import EquityFK, StringIndexed, DateIndexed
from ziplime.core.db.base_model import BaseModel


class EquitySupplementaryMapping(BaseModel):
    __tablename__ = "equity_supplementary_mappings"

    sid: Mapped[EquityFK]
    field: Mapped[StringIndexed]
    start_date: Mapped[DateIndexed]
    end_date: Mapped[DateIndexed]
    value: Mapped[str]

    __table_args__ = (
        PrimaryKeyConstraint('sid', 'field', "start_date"),
    )
