import datetime
from dataclasses import dataclass

from ziplime.assets.domain.bond_event_type import BondEventType
from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class BondEvent:
    """One dated event in a bond's schedule.

    Deliberately a single flat record rather than a class per event type: this is what the vendors
    send (Finam's ``GetPastBondsEvents`` returns one list with a ``type`` discriminator and a
    detail object per type), and one table keeps the schedule queryable by date in one pass.
    Fields that belong to another event type are simply ``None``.

    Attributes:
        asset: The bond this event belongs to.
        event_type: What kind of event this is.
        date: The payment date -- when cash actually moves.
        value: Money one bond pays, in ``currency``. Zero for an event that pays nothing.
        record_date: Who gets paid is settled at the close of this date. ``None`` means the
            payment date itself decides.
        period_start_date: Start of the accrual period a coupon covers. This is what makes accrued
            interest exact rather than an approximation from the annual rate.
        face_value: Face value the coupon was computed against.
        value_percent: The coupon as an annualised percentage of face value.
        new_face_value: Face value remaining after an amortization payment.
        initial_face_value: Face value the amortization percentage refers to.
        amortization_percent: Share of the initial face value this instalment repays.
        offer_type: ``put`` or ``call`` for an offer window.
        offer_price: Price the offer is exercised at, as quoted.
        offer_start_date, offer_end_date: The window in which the offer may be exercised.
        offer_agent: Broker acting as the issuer's agent for the offer.
    """

    asset: Asset
    event_type: BondEventType
    date: datetime.date
    value: float = 0.0
    currency: str = "RUB"

    record_date: datetime.date | None = None
    period_start_date: datetime.date | None = None
    face_value: float | None = None
    value_percent: float | None = None

    new_face_value: float | None = None
    initial_face_value: float | None = None
    amortization_percent: float | None = None

    offer_type: str | None = None
    offer_price: float | None = None
    offer_start_date: datetime.date | None = None
    offer_end_date: datetime.date | None = None
    offer_agent: str | None = None

    id: int | None = None

    @property
    def entitlement_date(self) -> datetime.date:
        """The date holdings are measured on to decide who receives this payment."""
        return self.record_date or self.date

    @property
    def pays_cash(self) -> bool:
        return self.event_type.pays_cash and self.value != 0.0

    def __str__(self):
        return f"{self.event_type.value}@{self.date} {self.value} {self.currency}"
