from enum import Enum


class PositionSide(Enum):
    """
    Enum representing position sides in derivatives trading.
    """
    LONG = "LONG"  # Profit from price increases
    SHORT = "SHORT"  # Profit from price decreases
    BOTH = "BOTH"  # Special case for certain APIs/modes
    NET = "NET"  # Net position across multiple positions
