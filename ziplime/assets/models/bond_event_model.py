import datetime

from sqlalchemy.orm import Mapped, mapped_column

from ziplime.core.db.annotated_types import AssetRouterFK, DateIndexed, IntegerPK
from ziplime.core.db.base_model import BaseModel


class BondEventModel(BaseModel):
    """One row per coupon, amortization, maturity or offer.

    ``date`` and ``record_date`` are both indexed: the simulation asks "what is payable today" and
    "whose holdings decide today's payment" once per session, for every bond held.
    """

    __tablename__ = "bond_events"

    id: Mapped[IntegerPK]
    asset_id: Mapped[AssetRouterFK]

    event_type: Mapped[str]
    date: Mapped[DateIndexed]
    value: Mapped[float]
    currency: Mapped[str]

    record_date: Mapped[datetime.date | None] = mapped_column(index=True)
    period_start_date: Mapped[datetime.date | None]
    face_value: Mapped[float | None]
    value_percent: Mapped[float | None]

    new_face_value: Mapped[float | None]
    initial_face_value: Mapped[float | None]
    amortization_percent: Mapped[float | None]

    offer_type: Mapped[str | None]
    offer_price: Mapped[float | None]
    offer_start_date: Mapped[datetime.date | None]
    offer_end_date: Mapped[datetime.date | None]
    offer_agent: Mapped[str | None]
