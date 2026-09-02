import datetime
from dataclasses import dataclass

from ziplime.assets.domain.day_count import DayCount
from ziplime.assets.domain.price_quotation import PriceQuotation
from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class Bond(Asset):
    """A debt security: a stream of coupons plus repayment of principal.

    Three properties separate a bond from an equity in a backtest, and getting any of them wrong
    silently distorts every number the simulation produces:

    * **The quote is not the price.** ``price_quotation`` says whether the tape carries money or a
      percentage of ``face_value``; on most exchanges it is the latter. See
      :class:`~ziplime.assets.domain.price_quotation.PriceQuotation`.
    * **The buyer pays accrued interest.** A bond changes hands at its *clean* price plus the
      coupon accrued since the last payment (НКД), which goes to the seller. Ignoring it makes
      every purchase look cheaper and every sale poorer than it was.
    * **The principal is not constant.** An amortizing issue repays face value in instalments, so
      ``face_value`` is the nominal *at issue* and the outstanding amount has to be read from the
      schedule -- :meth:`~ziplime.finance.bonds.BondBook.face_value` does that.

    Attributes:
        face_value: Nominal at issue, in ``quote_currency``. 1000 for a typical issue.
        maturity_date: When the outstanding principal is repaid.
        coupon_rate: Annual coupon as a fraction of face value (``0.0825`` for 8.25%). Zero for a
            discount bond, and only indicative for a floater -- the schedule is authoritative.
        coupon_frequency: Coupon payments per year; 0 for a zero-coupon bond.
        quote_currency: Currency the bond is quoted and pays in.
        day_count: Convention used to accrue interest when no coupon schedule is stored.
        price_quotation: Whether the quote is money or a percentage of face value.
        is_amortized: Whether the issue repays principal in instalments before maturity.
    """

    face_value: float
    maturity_date: datetime.date
    coupon_rate: float = 0.0
    coupon_frequency: int = 0
    quote_currency: str = "RUB"
    day_count: DayCount = DayCount.ACT_365
    price_quotation: PriceQuotation = PriceQuotation.PERCENT_OF_FACE
    is_amortized: bool = False

    def __hash__(self):
        return hash(self.id)

    @property
    def is_zero_coupon(self) -> bool:
        """True for a discount bond: no coupons, all of the return is price pulling to par."""
        return self.coupon_frequency == 0 or self.coupon_rate == 0.0

    @property
    def annual_coupon_amount(self) -> float:
        """Money one bond pays in coupons per year at the nominal rate."""
        return self.face_value * self.coupon_rate

    def coupon_amount(self) -> float:
        """Money one coupon pays, at the nominal rate and stated frequency."""
        if self.coupon_frequency <= 0:
            return 0.0
        return self.annual_coupon_amount / self.coupon_frequency
