"""How a futures contract settles at expiry."""
import enum


class SettlementType(enum.Enum):
    """Cash settlement versus physical delivery.

    The distinction is not cosmetic. A cash-settled contract can be held to the last session and
    simply pays out the difference. A physically delivered one turns into an obligation to deliver
    or receive the underlying -- barrels, shares, gas at Henry Hub -- so a position in it **must**
    be closed before the delivery window opens. Backtests that ignore this quietly assume a
    position can be carried through delivery, which no cash-settled-style P&L calculation
    describes.
    """

    #: Settles against a reference price; nothing changes hands but money.
    CASH = "CASH"
    #: Delivers the underlying. Must be closed before the notice date.
    PHYSICAL = "PHYSICAL"

    @property
    def is_deliverable(self) -> bool:
        return self is SettlementType.PHYSICAL
