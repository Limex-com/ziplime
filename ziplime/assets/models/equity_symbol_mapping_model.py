import datetime

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import IntegerPK, StringIndexed, ExchangeFK, EquityFK
from ziplime.core.db.base_model import BaseModel


class EquitySymbolMappingModel(BaseModel):
    __tablename__ = "equity_symbol_mappings"

    id: Mapped[IntegerPK]
    sid: Mapped[EquityFK]
    symbol: Mapped[str]
    company_symbol: Mapped[StringIndexed]  # The company identifier
    share_class_symbol: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    exchange: Mapped[ExchangeFK | None]
