"""Events on a bond's life: what the issuer pays, and when the terms change."""
import enum


class BondEventType(enum.Enum):
    """A dated event in a bond's schedule.

    Two of them move money on their own: :attr:`COUPON` and :attr:`AMORTIZATION`.

    :attr:`MATURITY` does **not**. It marks the date the issuer repays the last of the principal,
    and the repayment is booked by the ledger at par against whatever is still outstanding --
    which after every earlier instalment is exactly that final amount. Paying the event *and*
    redeeming the position would count the principal twice.

    Vendors do not necessarily send this type. A connector that reports redemption as a final
    principal instalment is expected to mark it as :attr:`MATURITY` on the way in, so that the
    engine sees one redemption however the vendor spelled it.

    :attr:`OFFER` does not pay either -- it is a date on which the holder may put the bond back to
    the issuer (or the issuer may call it), which a strategy can act on but the engine will not
    exercise on its own.
    """

    #: Periodic interest payment.
    COUPON = "COUPON"
    #: Partial repayment of principal; the face value drops by the amount repaid.
    AMORTIZATION = "AMORTIZATION"
    #: Final repayment of the outstanding face value.
    MATURITY = "MATURITY"
    #: A put/call window (оферта). Informational: the engine never exercises it for you.
    OFFER = "OFFER"
    #: A dividend-like payment some structured issues make; treated as cash, like a coupon.
    DIVIDEND = "DIVIDEND"
    #: The vendor sent an event whose type we do not model. Never pays cash.
    UNSPECIFIED = "UNSPECIFIED"

    @property
    def pays_cash(self) -> bool:
        """Whether holding the bond through this event credits cash *by itself*.

        False for :attr:`MATURITY`: the ledger repays the outstanding principal on that date, and
        crediting the event as well would pay the principal twice.
        """
        return self in (BondEventType.COUPON, BondEventType.AMORTIZATION,
                        BondEventType.DIVIDEND)

    @classmethod
    def from_api(cls, value: str | None) -> "BondEventType":
        """Parse a vendor's event type, tolerating names we do not know.

        Finam sends bare names (``COUPON``) and prefixed ones (``BOND_EVENT_TYPE_COUPON``), and an
        unknown value must not abort an ingest of a thousand events, so it degrades to
        :attr:`UNSPECIFIED` rather than raising.
        """
        if not value:
            return cls.UNSPECIFIED
        name = str(value).strip().upper()
        for prefix in ("BOND_EVENT_TYPE_", "EVENT_TYPE_", "TYPE_"):
            if name.startswith(prefix):
                name = name[len(prefix):]
        aliases = {
            "AMORT": cls.AMORTIZATION,
            "AMORTISATION": cls.AMORTIZATION,
            "REDEMPTION": cls.MATURITY,
            "MATURITY_DATE": cls.MATURITY,
            "PUT": cls.OFFER,
            "CALL": cls.OFFER,
            "OFFERTA": cls.OFFER,
        }
        if name in aliases:
            return aliases[name]
        try:
            return cls(name)
        except ValueError:
            return cls.UNSPECIFIED
