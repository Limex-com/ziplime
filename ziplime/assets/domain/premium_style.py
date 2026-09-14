"""How an option's premium is settled -- the one thing that makes OPRA and MOEX different books.

Two venues can list the same instrument, quote it the same way, and account for it completely
differently. A SPY call bought on OPRA is **paid for**: the premium leaves the account at the
trade, and the position is an asset from then on. A Sber call bought on MOEX is not paid for at
all; it accrues variation margin daily, exactly like a futures position, and both sides post
initial margin for the privilege.

That is not a detail of presentation. Run a margined option through premium accounting and the
buyer is charged money that never left their account and the position is valued twice -- once as
an asset and once through the margin flow. Run a premium option through margined accounting and
the purchase is free. Neither error raises; both change every number in the result.

The distinction has several names in the wild and they all mean this:

======================  ==========================================================
this enum               also called
======================  ==========================================================
``UPFRONT``             equity-style, stock-type settlement, premium-paid
``MARGINED``            futures-style settlement, futures-type margining,
                        margined options (MOEX/FORTS)
======================  ==========================================================

The engine already had both mechanisms -- they are what separates an equity from a futures
contract in :mod:`ziplime.finance.domain.ledger` -- so a margined option simply joins the path
futures already take, rather than getting one of its own.
"""
import enum


class PremiumStyle(enum.Enum):
    """Whether an option's premium is paid at the trade or margined daily."""

    #: The premium changes hands at the trade. The buyer's cash falls by
    #: ``price x multiplier x amount`` and the position is worth that much from then on; a short
    #: position is a liability of the same size. US equity and ETF options work this way, and it is
    #: the default because it is what "buying an option" means to most people.
    UPFRONT = "UPFRONT"

    #: No premium changes hands. The position accrues variation margin every session on the change
    #: in the option's own price -- ``(price - previous price) x multiplier x amount`` -- and has
    #: no inherent value, exactly like a futures position. Both the long and the short post initial
    #: margin. This is how MOEX/FORTS options settle.
    MARGINED = "MARGINED"

    @property
    def is_margined(self) -> bool:
        """True when the position settles variation margin instead of carrying a value."""
        return self is PremiumStyle.MARGINED

    @property
    def pays_premium_at_trade(self) -> bool:
        return self is PremiumStyle.UPFRONT
