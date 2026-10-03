"""Call or put."""
import enum


class OptionType(enum.Enum):
    """Which side of the strike an option pays off on.

    The whole instrument follows from this one bit: a call is worth ``max(S - K, 0)`` at expiry
    and a put ``max(K - S, 0)``, so every payoff, every Greek and every exercise decision in this
    package branches on it. It is also the letter in the OCC symbol -- ``SPY240614C00523000`` --
    which is how a chain from any real feed identifies a contract.
    """

    CALL = "CALL"
    PUT = "PUT"

    @property
    def letter(self) -> str:
        """``C`` or ``P``, as it appears in an OCC symbol."""
        return self.value[0]

    @property
    def sign(self) -> int:
        """``+1`` for a call, ``-1`` for a put.

        The direction the payoff moves with the underlying, which turns the two payoff formulas
        into one: ``max(sign * (S - K), 0)``.
        """
        return 1 if self is OptionType.CALL else -1

    @property
    def vollib_flag(self) -> str:
        """The ``'c'``/``'p'`` flag every ``vollib`` entry point takes."""
        return self.letter.lower()

    @classmethod
    def from_letter(cls, letter: str) -> "OptionType":
        """Parse ``C``/``P`` (in either case), as found in an OCC symbol."""
        try:
            return {"C": cls.CALL, "P": cls.PUT}[letter.upper()]
        except KeyError:
            raise ValueError(f"Option type must be C or P, got {letter!r}.") from None

    def intrinsic_value(self, underlying_price: float, strike: float) -> float:
        """What one unit of the option is worth if it expired right now, before the multiplier."""
        return max(self.sign * (underlying_price - strike), 0.0)
