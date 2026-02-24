from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import StringPK, ExchangeFK
from ziplime.core.db.base_model import BaseModel


class FuturesRootSymbol(BaseModel):
    __tablename__ = "futures_root_symbols"

    root_symbol: Mapped[StringPK]
    root_symbol_id: Mapped[int]
    sector: Mapped[str]
    description: Mapped[str]
    exchange: Mapped[ExchangeFK]
