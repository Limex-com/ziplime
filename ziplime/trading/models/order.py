from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import UuidPK
from ziplime.core.db.base_model import BaseModel


class OrderModel(BaseModel):
    id: Mapped[UuidPK]

    base_asset_symbol: Mapped[str]
    quote_asset_symbol: Mapped[str]

