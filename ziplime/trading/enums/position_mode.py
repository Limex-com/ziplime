from enum import Enum


class PositionMode(Enum):
    """
    Enum representing position modes in derivative trading.
    """
    HEDGE = "HEDGE"              # Can hold both long and short positions simultaneously
    ONEWAY = "ONEWAY"            # Can only hold either long or short position at a time
    PORTFOLIO_MARGIN = "PORTFOLIO_MARGIN"  # Uses portfolio risk assessment for margin calculation
    ISOLATED = "ISOLATED"        # Each position's margin is isolated from others
    CROSS = "CROSS"              # Positions share margin across the account
