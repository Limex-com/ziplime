"""How a listing's quoted price relates to money."""
import enum


class PriceQuotation(enum.Enum):
    """Whether a quoted price *is* money, or a percentage of something else.

    Equities and futures quote money per unit; the number on the tape multiplied by the position
    size (and the multiplier) is the exposure. Bonds do not. MOEX, like most exchanges, quotes a
    bond as a **percentage of its face value**: ``98.42`` means 98.42% of the nominal, so a lot of
    ten 1000-rouble bonds at that quote is 9842 roubles, not 984.20. Reading a bond quote as money
    understates every bond position by a factor of roughly ``face_value / 100`` -- for a standard
    1000-rouble nominal, a factor of ten.
    """

    #: The quote is money per unit, e.g. an equity at 312.5 RUB per share.
    MONEY = "MONEY"
    #: The quote is a percentage of face value, e.g. a bond at 98.42% of a 1000 nominal.
    PERCENT_OF_FACE = "PERCENT_OF_FACE"

    def money_price(self, quoted_price: float, face_value: float) -> float:
        """Convert one unit's quoted price into money."""
        if self is PriceQuotation.PERCENT_OF_FACE:
            return quoted_price / 100.0 * face_value
        return quoted_price

    def quoted_price(self, money_price: float, face_value: float) -> float:
        """Inverse of :meth:`money_price`: what quote corresponds to a money price."""
        if self is PriceQuotation.PERCENT_OF_FACE:
            if face_value == 0:
                return 0.0
            return money_price / face_value * 100.0
        return money_price
