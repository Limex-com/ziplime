import datetime

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import IntegerPK, StringIndexed, ExchangeFK, EquityFK, CurrencyFK
from ziplime.core.db.base_model import BaseModel


class CurrencySymbolMappingModel(BaseModel):
    __tablename__ = "currency_symbol_mappings"

    id: Mapped[IntegerPK]
    sid: Mapped[CurrencyFK]
    symbol: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    exchange: Mapped[ExchangeFK | None]
